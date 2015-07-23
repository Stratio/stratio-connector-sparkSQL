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
package com.stratio.connector.sparksql.core.engine.query

import akka.actor.{Stash, Props, ActorRef, Actor}
import com.stratio.connector.sparksql.core.SparkSQLConnector
import com.stratio.connector.sparksql.core.`package`.SparkSQLContext
import com.stratio.connector.sparksql.core.connection.ConnectionHandler
import com.stratio.connector.commons.{Loggable, Metrics}
import com.stratio.connector.sparksql.core.engine.query.QueryExecutor.DataFrameProvider
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.data.TableName
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow
import com.stratio.connector.commons.timer
import com.stratio.connector.sparksql.core._

/**
 * A QueryManager deploys a bunch of query executors.
 * It's in charge of orchestrating them and rooting job requests.
 * @param executorsAmount Amount of query executors
 * @param sqlContext SQLContext
 * @param provider A DataFrame provider, that is, the way to create a DataFrame
 *                 given a certain context.
 * @param connectionHandler The connection handler.
 */
class QueryManager(
  executorsAmount: Int,
  sqlContext: SparkSQLContext,
  provider: DataFrameProvider,
  connectionHandler: ConnectionHandler) extends Actor
with Stash
with Loggable
with Metrics
with Constants{

  import SparkSQLConnector._
  import QueryManager._
  import timer._

  type QueryId = String
  type QueryExecutorRef = ActorRef
  type PendingQueriesMap = Map[QueryId, QueryExecutorRef]

  //  All query executors are busy or not
  var busy: Boolean = false

  /** Relation of pending queries and executors that are executing them.*/
  var pendingQueries: PendingQueriesMap = Map()

  /** Query executors that remain free*/
  var freeExecutors: Set[QueryExecutorRef] = {
    logger.info(s"Created query executors pool $executorsAmount-sized")
    timeFor("Created query executors pool.") {
      (1 to executorsAmount).map(_ =>
        context.actorOf(QueryExecutor(
          sqlContext,
          connectorConfig.get.getInt(ChunkSize),
          provider,
          connectionHandler,
          connectorConfig.get.getBoolean(AsyncStoppable)))).toSet
    }
  }

  /** Registered tables*/
  var registeredTables: Set[TableName] = Set()

  override def receive: Receive = {

    case job @ SyncExecute(_,workflow) =>
      logger.info(s"[QueryManager] Processed job request : [$job]")
      val (registered,notRegistered) = QueryEngine
        .involvedTables(workflow)
        .partition(table => registeredTables.contains(table))
      if (notRegistered.isEmpty){
        val requester = sender()
        executeJob(job,requester)
      } else {
        logger.warn(s"Not all involved tables are registered: $notRegistered")
        self forward job
      }


    case job: AsyncJob =>
      logger.info(s"[QueryManager] Processed job request : [$job]")
      val requester = sender()
      executeJob(job,requester)

    case Stop(queryId) =>
      logger.info(s"[QueryManager] Stopped query [$queryId]")
      timeFor(s"[QueryManager] Stopped query") {
        finish(queryId, stopActor = true)
      }

    case Finished(queryId) =>
      logger.info(s"[QueryManager] Set query $queryId as finished")
      timeFor(s"[QueryManager] Set query finished") {
        finish(queryId)
      }

    case Registered(table) =>
      logger.info(s"[QueryManager] Registered table ${table.getQualifiedName}")
      registeredTables += table

    case Unregistered(table) =>
      logger.info(s"[QueryManager] Unregistered table ${table.getQualifiedName}")
      registeredTables -= table

    case other => println(s"Unexpected: $other [${other.getClass}]")
  }

  //  Helpers

  /**
   * Assign any type of job to first free executor
   * @param job Job to be executed
   * @param requester ActorRef that requests this query execution.
   */
  def executeJob(job: Job,requester: ActorRef): Unit = {
    timeFor(s"[QueryManager] Processed job request.") {
      if (busy) stash()
      else assignJob(job,requester)
    }
  }

  /**
   * Assign a new async query execution to some free executor.
   * It implies updating freeWorkers, busyWorkers
   * and pendingQueries collections
   *
   * @param msg The new async query to be executed
   */
  def assignJob(msg: Job,sender: ActorRef): Unit = {
    val (executor, rest) = freeExecutors.splitAt(1)
    freeExecutors = rest
    pendingQueries += (msg.queryId -> executor.head)
    executor.head forward msg
    if (freeExecutors.isEmpty) busy = true
  }

  /**
   * Mark current query as finished (even if it's not finished yet).
   * In that case, it will send an Stop message to its query executor.
   *
   * @param queryId Query finished or to be finished
   * @param stopActor Flag for asking executor to stop its async job.
   */
  def finish(
    queryId: QueryId,
    stopActor: Boolean = false): Unit = {
    val executor = pendingQueries(queryId)
    pendingQueries -= queryId
    freeExecutors += executor
    if (stopActor) executor ! Stop(queryId)
    if (busy) {
      busy = false
      unstashAll()
    }
  }

}

object QueryManager {

  def apply(
    executorsAmount: Int,
    sqlContext: SparkSQLContext,
    connectionHandler: ConnectionHandler,
    provider: DataFrameProvider): Props =
    Props(new QueryManager(executorsAmount, sqlContext, provider, connectionHandler))

  //  Messages

  sealed trait Job {

    def queryId: QueryManager#QueryId

    def workflow: LogicalWorkflow

  }

  sealed trait AsyncJob extends Job {

    def resultHandler: IResultHandler

    def currentChunk: Option[Int] = None

  }

  case class SyncExecute(
    queryId: QueryManager#QueryId,
    workflow: LogicalWorkflow) extends Job
  
  case class PagedExecute(
    queryId: QueryManager#QueryId,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler,
    pageSize: Int) extends AsyncJob {
    override def currentChunk = Some(pageSize)
  }

  case class AsyncExecute(
    queryId: QueryManager#QueryId,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler) extends AsyncJob

  case class Stop(
    queryId: QueryManager#QueryId)

  case class Finished(
    queryId: QueryManager#QueryId)

  case class Registered(
    table: TableName)

  case class Unregistered(
    table: TableName)

}
