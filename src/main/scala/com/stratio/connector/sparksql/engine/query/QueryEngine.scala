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

import scala.collection.JavaConversions._
import akka.actor.ActorRef
import org.apache.spark.sql.SchemaRDD
import com.stratio.connector.commons.timer
import com.stratio.connector.sparksql.{Loggable, Provider, SparkSQLContext}
import com.stratio.connector.sparksql.CrossdataConverters._
import com.stratio.crossdata.common.data.TableName
import com.stratio.crossdata.common.metadata.ColumnMetadata
import com.stratio.connector.sparksql.engine.query.QueryManager._
import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IQueryEngine, IResultHandler}
import com.stratio.crossdata.common.logicalplan.{Project, Select, LogicalWorkflow}
import com.stratio.crossdata.common.result.QueryResult

/**
 * Query engine that support async., paged or sync. queries
 *
 * @param sqlContext SparkSQLContext instance.
 * @param queryManager Reference to asynchronous job executors manager.
 * @param config ConnectorClusterConfig instance.
 * @param provider DataSource provider.
 */
case class QueryEngine(
  sqlContext: SparkSQLContext,
  queryManager: ActorRef,
  config: ConnectorClusterConfig,
  provider: Provider) extends IQueryEngine with Loggable {

  import QueryEngine._
  import timer._

  override def execute(workflow: LogicalWorkflow): QueryResult = {
    val rdd = time(s"Executing sync. query\n\t${workflow.toString}") {
      executeQuery(workflow, sqlContext, config, provider)
    }
    time(s"Processing unique query result...") {
      QueryResult.createQueryResult(
        toResultSet(rdd, toColumnMetadata(workflow)), 0, true)
    }
  }

  override def pagedExecute(
    queryId: String,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler,
    pageSize: Int): Unit =
    time(s"Executing paged query [$queryId]\n\t${workflow.toString}") {
      queryManager ! PagedExecute(queryId, workflow, resultHandler, pageSize)
    }


  override def asyncExecute(
    queryId: String,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler): Unit =
    time(s"Executing async. query [$queryId]\n\t${workflow.toString}") {
      queryManager ! AsyncExecute(queryId, workflow, resultHandler)
    }


  override def stop(queryId: String): Unit =
    time(s"Stopping query [$queryId]") {
      queryManager ! Stop(queryId)
    }

}

object QueryEngine extends Loggable {

  type Query = String

  //  Common functions

  /**
   * Execute some query from given workflow.
   *
   * @param workflow Given workflow.
   * @param sqlContext Targeted SQL context.
   * @param config Connector cluster configuration.
   * @param provider The targeted data store.
   * @return Obtained schema RDD.
   */
  def executeQuery(
    workflow: LogicalWorkflow,
    sqlContext: SparkSQLContext,
    config: ConnectorClusterConfig,
    provider: Provider): SchemaRDD = {
    import scala.collection.JavaConversions._
    import timer._
    //  Register table if it doesn't already exist in catalog...
    workflow.getInitialSteps.map { case project: Project =>
      registerTable(
        qualified(project.getTableName),
        sqlContext,
        provider,
        globalOptions(config))
    }
    //  Extract raw query from workflow
    val query = time(s"Getting workflow plain query ...") {
      workflow.getSqlDirectQuery
    }
    logger.debug(s"Workflow plain query : $query")
    //  Format query for avoiding conflicts such as 'catalog.table' issue
    val formattedQuery = time("Formatting query to SparkSQL format") {
      sparkSQLFormat(query)
    }
    logger.debug(s"Workflow SparkSQL formatted query : $query")
    //  Execute actual query ...
    /*TODO Adapt Crossdata queries

      FROM

      SELECT highschool.pupils.id,
      highschool.pupils.name,
      highschool.pupils.surname,
      highschool.pupils.age,
      highschool.pupils.enrolled FROM highschool.pupils

      TO

      SELECT `_2.id`,`_2.name`,`_2.surname`,`_2.age`,`_2.enrolled` FROM pupils

      */
    val rdd = sqlContext.sql(formattedQuery)
    logger.debug(rdd.schemaString)
    logger.debug(s"${rdd.collect().toList}")
    //  ... and format its structure for adapting it to provider's.
    time("Formatting RDD for discharding metadata fields") {
      provider.formatRDD(
        rdd,
        sqlContext)
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
    //  Get column selectors from last step (SELECT)
    val (columnTypes, selectors) = workflow.getLastStep match {
      case s: Select => (s.getTypeMap, s.getOutputSelectorOrder)
    }
    //  Map them into ColumnMetadata
    selectors.map(s =>
      new ColumnMetadata(
        s.getColumnName, Array(),
        columnTypes(s.getColumnName.getName))).toList
  }

  /**
   * Maps catalog.table names that use dots into some other without them.
   *
   * @param statement Query statement.
   * @return Escaped query statement
   */
  def sparkSQLFormat(statement: Query, conflictChar: String = "."): Query = {
    val regexp = s"(\\w*)[$conflictChar](\\w*)".r
    val (fields, tables) = regexp.findAllIn(statement)
      .toList.distinct.partition(_.startsWith(conflictChar))
    val withFormattedTables = (statement /: tables)((statement, table) =>
      statement.replace(table, table.split("\\.").last))
    (withFormattedTables /: fields)((statement, field) =>
      statement.replace(field, s"._2$field"))
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
    if (sqlContext.getCatalog.tableExists(Seq(tableName)))
      logger.warn(s"Tried to register $tableName table but it already exists!")
    else {
      logger.debug(s"Registering table [$tableName]")
      val statement = createTemporaryTable(
        tableName,
        provider,
        options)
      logger.debug(s"Statement: $statement")
      sqlContext.sql(statement)
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
      logger.debug(s"Unregistering table [$tableName]")
      sqlContext.getCatalog.unregisterTable(seqName)
    }
  }

  def globalOptions(config: ConnectorClusterConfig): Map[String, String] =
    config.getClusterOptions.toMap ++ config.getConnectorOptions.toMap

  /*
   *  Provides the necessary syntax for creating a temporary table in SparkSQL.
   */
  private def createTemporaryTable(
    table: String,
    provider: Provider,
    options: Map[String, String]): String =
    s"""
       |CREATE TEMPORARY TABLE $table
        |USING ${provider.datasource}
        |OPTIONS (${options.map { case (k, v) => s"$k '$v'"}.mkString(",")})
       """.stripMargin
}