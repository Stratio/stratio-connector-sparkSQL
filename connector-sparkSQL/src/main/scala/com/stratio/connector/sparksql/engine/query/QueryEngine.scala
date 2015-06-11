/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.sparksql.engine.query

import com.stratio.connector.sparksql.connection.ConnectionHandler
import com.stratio.connector.sparksql.providers.Provider
import com.stratio.crossdata.common.statements.structures.{FunctionSelector, Selector}
import org.apache.spark.partial.PartialResult
import org.apache.spark.sql.hive.HiveContext
import com.stratio.connector.sparksql.providers._
import scala.collection.JavaConversions._
import akka.actor.ActorRef
import org.apache.spark.sql.DataFrame
import com.stratio.crossdata.common.data.{ClusterName, TableName}
import com.stratio.crossdata.common.metadata.{TableMetadata, ColumnMetadata}
import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IQueryEngine, IResultHandler}
import com.stratio.crossdata.common.logicalplan.{Project, Select, LogicalWorkflow}
import com.stratio.crossdata.common.result.QueryResult
import com.stratio.crossdata.common.statements.structures.{FunctionSelector, Selector}
import com.stratio.connector.commons.timer
import com.stratio.connector.commons.{Loggable, Metrics}
import com.stratio.connector.sparksql.connection.ConnectionHandler
import com.stratio.connector.sparksql.providers.{CustomContextProvider, Provider}
import com.stratio.connector.sparksql._
import com.stratio.connector.sparksql.CrossdataConverters._
import com.stratio.connector.sparksql.engine.query.QueryManager._

import scala.util.Try

/**
 * Query engine that support async., paged or sync. queries
 *
 * @param sqlContext SparkSQLContext instance.
 * @param queryManager Reference to asynchronous job executors manager.
 * @param connectionHandler ConnectorClusterConfig instance.
 */
case class QueryEngine(
  sqlContext: SparkSQLContext,
  queryManager: ActorRef,
  connectionHandler: ConnectionHandler) extends IQueryEngine
with Loggable
with Metrics {

  import QueryEngine._
  import timer._

  override def execute(workflow: LogicalWorkflow): QueryResult = {

    logger.info(s"Execute workflow [$workflow]. The direct query is [${workflow.getSqlDirectQuery}]")

    val dataFrame = timeFor(s"Sync. query executed.") {
      executeQuery(workflow, sqlContext, connectionHandler)
    }
    timeFor(s"Unique query result processed.") {
      QueryResult.createQueryResult(
        toResultSet(dataFrame, toColumnMetadata(workflow)), 0, true)
    }
  }

  override def pagedExecute(
    queryId: String,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler,
    pageSize: Int): Unit = {

    logger.info(s"Paged execute workflow [$workflow]. The direct query is [${workflow.getSqlDirectQuery}]")

    timeFor(s"Paged query [$queryId] executed.") {
      queryManager ! PagedExecute(queryId, workflow, resultHandler, pageSize)
    }
  }


  override def asyncExecute(
    queryId: String,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler): Unit = {

    logger.info(s"Async execute workflow [$workflow]. The direct query is [${workflow.getSqlDirectQuery}]")

    timeFor(s"Async. query [$queryId] executed.") {
      queryManager ! AsyncExecute(queryId, workflow, resultHandler)
    }
  }


  override def stop(queryId: String): Unit = {
    logger.info(s"Query [$queryId] stopped.")
    timeFor(s"Query stop.") {
      queryManager ! Stop(queryId)
    }
  }

}

object QueryEngine extends Loggable with Metrics {

  type Query = String

  //  Common functions

  /**
   * Execute some query from given workflow.
   *
   * @param workflow Given workflow.
   * @param sqlContext Targeted SQL context.
   * @param connectionHandler Connection handler for all attached clusters.
   * @param metadataTimeout Timeout for querying connector actor.
   * @return Obtained DataFrame.
   */
  def executeQuery(
    workflow: LogicalWorkflow,
    sqlContext: SparkSQLContext,
    connectionHandler: ConnectionHandler,
    metadataTimeout: Int = 3000): DataFrame = {
    import timer._
    withProjects(connectionHandler, workflow) { projects =>
      //  Extract raw query from workflow
      val query = timeFor(s"Got workflow plain query.") {
        workflow.getSqlDirectQuery
      }
      logger.debug(s"Workflow plain query before format : [$query]")
      //  Format query for adapting it to involved providers
      val providedProjects = for {
        project <- projects
        (dataStore, globalOptions) <- projectInfo(connectionHandler, project, metadataTimeout)
        provider <- providers.apply(dataStore)
      } yield (provider, globalOptions)
      val providersFormatted = (query /: providedProjects) {
        case (statement, (provider, options)) => provider.formatSQL(statement, options)
      }
      logger.info(s"SparkSQL query after providers format: [$providersFormatted]")
      //  Format query for avoiding conflicts such as 'catalog.table' issue
      val formattedQuery = timeFor("Query formatted to SparkSQL format") {
        sparkSQLFormat(providersFormatted, catalogsFromWorkflow(workflow))
      }
      logger.info(s"Query after general format: [$formattedQuery]")
      logger.info("Find for partialResults")
      val partialsResults = PartialResultProcessor().recoveredPartialResult(workflow)
      logger.info(("Creating new hive context"))
      val hiveContext = new HiveContext(sqlContext.sparkContext) with Catalog
      // if partial results have found, register temp table
      val catalogsPartialResult = partialsResults.map {
        case (pr) => {
          val resultSet = pr.getResults
          val df = CrossdataConverters.toSchemaRDD(resultSet, hiveContext)
          val cm = resultSet.getColumnMetadata.get(0).getName
          val catalogName = cm.getTableName.getCatalogName.getName

          val tableName = cm.getTableName.getName
          df.registerTempTable(tableName)
          catalogName
        }

      }

      val partialResultsFormatted = timeFor("SparkSQL query after partial results format:   ") {
        sparkSQLFormat(formattedQuery, catalogsPartialResult.toList)
      }
      logger.info(s"SparkSQL query after result set format: [$partialResultsFormatted]")

      logger.info(s"SparkSQL query after providers format: [$partialResultsFormatted]")
      //  Execute actual query ...
      val dataframe = hiveContext.sql(partialResultsFormatted)
      logger.info("Spark has returned the execution to the SparkSQL Connector.")
      logger.debug(dataframe.schema.treeString)
      //Return dataFrame
      dataframe
    }
  }


  /**
   * Get columns metadata from workflow.
   *
   * @param workflow Given LogicalWorkflow
   * @return List of ColumnMetadata
   */
  def toColumnMetadata(workflow: LogicalWorkflow): List[ColumnMetadata] = {
    import scala.collection.JavaConversions._
    import com.stratio.connector.sparksql.engine.query.mappings.functionType
    logger.debug("Getting column selectors from last step (SELECT)")
    val (columnTypes: ColumnTypeMap, selectors: List[Selector]) = workflow.getLastStep match {
      case s: Select => s.getTypeMap.toMap -> s.getOutputSelectorOrder.toList
      case _ =>
        logger.warn(s"No LastStep found in [${workflow.getSqlDirectQuery}]. " +
          s"ColumnMetadata will be empty...")
        Map() -> List()
    }
    logger.debug(s"ColumnTypes : $columnTypes\nSelectors : $selectors")
    //  Map them into ColumnMetadata
    selectors.map {
      case fs: FunctionSelector =>
        new ColumnMetadata(fs.getColumnName, Array(), functionType(fs.getFunctionName))
      case s =>
        val columnName = s.getColumnName
        Option(s.getAlias).foreach(columnName.setAlias)
        new ColumnMetadata(
          columnName,
          Array(),
          columnTypes.getOrElse(s.getColumnName.getName, columnTypes(s.getAlias)))
    }


  }



  /**
   * Maps catalog.table names that use dots into some other without them.
   *
   * @param statement Query statement.
   * @return Escaped query statement
   */
  def sparkSQLFormat(
    statement: Query,
    catalogs: Iterable[String],
    conflictChar: String = "."): Query = {

    //  Remove catalog name

    val withoutCatalog = (statement /: catalogs) {
      case (s, catalog) =>
        val regex = s"[\\s|(]$catalog\\.".r
        regex.replaceAllIn(s,mtch =>
          mtch.toString().dropRight(s"$catalog.".length))
    }

    withoutCatalog
  }

  /**
   * Converts name to canonical format.
   *
   * @param name Table name.
   * @return Sequence of split parts from qualified name.
   */
  def qualified(name: TableName): String =
    name.getName

  /**
   * Register a table with its options in sqlContext catalog.
   * If table already exists, it throws a warning.
   *
   * @param tableName Table name.
   * @param sqlContext Targeted SQL context.
   * @param provider Targeted data source.
   * @param options Options map.
   */
  def registerTable(
    tableName: String,
    sqlContext: SparkSQLContext,
    provider: Provider,
    options: Map[String, String]): Unit = {

    //Aux register method, generic for no mather what SQLContext is being used
    def register(
      tableName: String,
      sqlContext: SparkSQLContext,
      provider: Provider,
      options: Map[String, String],
      temporaryTable: Boolean = false): Try[Unit] = Try[Unit] {
      if (sqlContext.getCatalog.tableExists(Seq("default", tableName))) {
        logger.warn(s"Tried to register $tableName table but it already exists!")
        unregisterTable(tableName, sqlContext)
        register(tableName, sqlContext, provider, options)
      }
      else {
        logger.debug(s"Registering table [$tableName]")
        val statement = createTable(
          tableName,
          provider,
          options,
          temporaryTable)
        logger.debug(s"Statement: $statement")
        sqlContext.sql(statement)
      }
    }.recover {
      case t: Throwable =>
        logger.error(s"Error at registering table '$tableName' : ${t.getMessage}")
    }

    provider match {
      case provider: CustomContextProvider[SparkSQLContext@unchecked] =>
        provider.sqlContext.foreach { context =>
          logger.debug(s"Registering $tableName into '${provider.dataSource}' specific context")
          register(tableName, context, provider, options, !provider.catalogPersistence)
          logger.debug(s"Retrieving table '$tableName' as dataFrame")
          val dataFrame = context.table(tableName)
          logger.debug(s"Registering dataFrame with schema ${dataFrame.schema} into common context'")
          sqlContext.createDataFrame(dataFrame.rdd, dataFrame.schema).registerTempTable(tableName)
        }
      case simpleProvider =>
        logger.debug(s"Registering $tableName into regular context")
        register(tableName, sqlContext, provider, options)
    }

  }

  /**
   * Unregister, if exists, given table name.
   *
   * @param tableName Table name.
   * @param sqlContext Targeted SQL context.
   */
  def unregisterTable(
    tableName: String,
    sqlContext: SparkSQLContext): Unit = {
    val seqName = Seq(tableName)
    if (!sqlContext.getCatalog.tableExists(seqName))
      logger.warn(s"Tried to unregister $tableName table but it already exists!")
    else {
      logger.info(s"Un-registering table [$tableName]")
      sqlContext.sql(s"DROP TABLE $tableName")
      //sqlContext.getCatalog.unregisterTable(seqName)
    }
  }

  type DataStore = String
  type GlobalOptions = Map[String, String]




  /**
   * Combine both connector and cluster options in a single map.
   * @param config Connector cluster configuration
   * @return The combined map
   */
  def globalOptions(config: ConnectorClusterConfig): GlobalOptions = {
config.getClusterOptions.toMap ++ config.getConnectorOptions.toMap
  }

  /**
   * Combine both connector and cluster options in a single map.
   * @param config Connector cluster configuration
   * @param tableMetadata the table metadata
   * @return The combined map
   */
  def globalOptions(config: ConnectorClusterConfig, tableMetadata : TableMetadata): GlobalOptions = {

    val tablePath = s"""${config.getClusterOptions.get("path")}/${tableMetadata.getName.getCatalogName.getName}/${tableMetadata.getName.getName}"""

    val map = globalOptions(config) + ("c_table" -> tableMetadata.getName.getName) + ("keyspace" ->tableMetadata.getName.getCatalogName.getName) + ("path" -> tablePath)
    map
  }

  /**
   * Retrieves project info and metadata options from given project and connection
   *
   * @param connectionHandler ConnectionHandler for retrieving related connection
   * @param project Workflow project
   * @param metadataTimeout Timeout for synchronous call
   * @return A possible pair of data store name and its options.
   */
  private def projectInfo(
    connectionHandler: ConnectionHandler,
    project: Project,
    metadataTimeout: Int): Option[(DataStore, GlobalOptions)] = {
    val cluster = project.getClusterName
    connectionHandler.getConnection(cluster.getName).map {
      case connection =>
        (connection.config.getDataStoreName.getName,
          globalOptions(connection.config) ++
            SparkSQLConnector.connectorApp
              .getTableMetadata(cluster, project.getTableName, metadataTimeout)
              .map(_.getOptions.toMap.map {
              case (k, v) => k.getStringValue -> v.getStringValue
            }).getOrElse(Map()))
    }
  }

  /**
   * Returns an Spark SQL script for creating a table.
   *
   * @param table Table name to be registered
   * @param provider Used dataSource for creating the table
   * @param options Options map to be used in table creation
   * @param temporary Is this one a temporary table?
   * @return An Spark SQL script.
   */
  private def createTable(
    table: String,

    provider: Provider,
    options: Map[String, String],
    temporary: Boolean = false): String = {

    val register = s"""
                      |CREATE ${if (temporary) "TEMPORARY" else ""} TABLE $table

        |USING ${provider.dataSource}

        |OPTIONS (${options.map { case (k, v) => s"$k '$v'" }.mkString(",")})
       """

      .stripMargin

        logger. info(register)
        register
}




  /**
   * Execute some statements assuring that current job will be started
   * at connectionHandler, besides it will be stopped as well at the end.
   *
   * @param connectionHandler ConnectionHandler of this connector.
   * @param clusters Involved clusters.
   * @param f Action to be executed.
   * @tparam T Action returning type.
   * @return Action result type.
   */
  def withProjects[T](
    connectionHandler: ConnectionHandler,
    clusters: Iterable[Project])(f: Iterable[Project] => T): T = {
    clusters.foreach(cluster => connectionHandler.startJob(cluster.getClusterName.getName))
    val result = f(clusters)
    clusters.foreach(cluster => connectionHandler.endJob(cluster.getClusterName.getName))
    result
  }

  /**
   * Execute some statements assuring that current job will be started
   * at connectionHandler, besides it will be stopped as well at the end.
   * (Based on generic {{{withProjects}}})
   *
   * @param connectionHandler ConnectionHandler of this connector.
   * @param workflow Workflow from Projects (attached clusters info) will be extracted
   * @param f Action to be executed
   * @tparam T Action returning type
   * @return Action result type
   */
  def withProjects[T](
    connectionHandler: ConnectionHandler,
    workflow: LogicalWorkflow)(f: Iterable[Project] => T): T = {
    withProjects(connectionHandler, workflow.getInitialSteps.map {
      case project: Project => project
    })(f)
  }

  def catalogsFromWorkflow(lw: LogicalWorkflow): Iterable[String] = {
    lw.getInitialSteps.map {
      case project: Project =>
        val catalogName = project.getCatalogName
        if (logger.isDebugEnabled)
          logger.debug(s"Catalog [$catalogName] has been find in the logicalWorkflow")
        catalogName
    }
  }

}