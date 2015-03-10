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

import com.stratio.connector.commons.timer
import com.stratio.connector.sparksql.{Loggable, Provider, SparkSQLContext}
import com.stratio.connector.sparksql.engine.query.TypeConverters._
import com.stratio.crossdata.common.metadata.ColumnMetadata
import org.apache.spark.sql.SchemaRDD
import akka.actor.ActorRef
import com.stratio.connector.sparksql.engine.query.QueryManager._
import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IQueryEngine, IResultHandler}
import com.stratio.crossdata.common.logicalplan.{Select, LogicalWorkflow}
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
    logger.debug(s"Workflow plain query : $query")
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