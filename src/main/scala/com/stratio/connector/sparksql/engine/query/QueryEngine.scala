package com.stratio.connector.sparksql.engine.query

import com.stratio.connector.commons.timer
import com.stratio.connector.sparksql.{Loggable, Provider, SparkSQLContext}
import com.stratio.connector.sparksql.engine.query.TypeConverters._
import com.stratio.crossdata.common.data.ColumnName
import com.stratio.crossdata.common.metadata.ColumnMetadata
import org.apache.spark.sql.SchemaRDD
import akka.actor.ActorRef
import com.stratio.connector.sparksql.engine.query.QueryManager._
import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IQueryEngine, IResultHandler}
import com.stratio.crossdata.common.logicalplan.{Select, Project, LogicalWorkflow}
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

  //  Common functions

  def executeQuery(
    workflow: LogicalWorkflow,
    sqlContext: SparkSQLContext,
    config: ConnectorClusterConfig,
    provider: Provider): SchemaRDD = {
    //  Extract raw query from workflow
    val query = workflow.getSqlDirectQuery
    //  Execute actual query
    sqlContext.sql(query)
  }

  def toColumnMetadata(workflow: LogicalWorkflow): List[ColumnMetadata] = {
    import scala.collection.JavaConversions._
    val (columnTypes, selectors) = workflow.getLastStep match {
      case s: Select => (s.getTypeMap, s.getOutputSelectorOrder)
    }
    selectors.map(s =>
      new ColumnMetadata(
        s.getColumnName, Array(),
        columnTypes(s.getColumnName.getName))).toList
  }

}