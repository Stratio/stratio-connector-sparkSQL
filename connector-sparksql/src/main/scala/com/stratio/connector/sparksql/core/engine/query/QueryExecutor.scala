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

import com.stratio.connector.sparksql.core.Configuration
import com.stratio.connector.sparksql.core.`package`.SparkSQLContext
import com.stratio.connector.sparksql.core.connection.ConnectionHandler
import com.stratio.crossdata.common.result.QueryResult
import com.stratio.connector.commons.timer
import org.apache.spark.sql.types.StructType
import scala.concurrent.duration._
import akka.actor.{Props, Actor}
import com.stratio.connector.commons.{Loggable, Metrics}
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow
import com.stratio.connector.sparksql.CrossdataConverters._
import org.apache.spark.sql.{Row, DataFrame}
import QueryEngine.toColumnMetadata
import QueryExecutor._

/**
 * Minimum query execution unit.
 *
 * @param sqlContext The SQLContext
 * @param defaultChunkSize Max row size in a chunk
 * @param provider SparkSQL Data source provider
 * @param asyncStoppable Whether its tasks can be stopped or not
 */
class QueryExecutor(
  sqlContext: SparkSQLContext,
  defaultChunkSize: Int,
  provider: DataFrameProvider,
  connectionHandler: ConnectionHandler,
  asyncStoppable: Boolean = true) extends Actor
with Loggable
with Metrics
with Configuration {

  import QueryManager._
  import timer._

  type Chunk = (Iterator[Row], Int)

  var currentJob: Option[AsyncJob] = None

  var currentSchema: Option[StructType] = None

  /** Current job chunks iterator */
  var rddChunks: Iterator[Chunk] = List.empty[Chunk].iterator

  /** Maximum time for waiting at count approx in chunk split */
  val timeoutCountApprox = connectorConfig.get.getInt(CountApproxTimeout).seconds

  override def receive = {

    case job@SyncExecute(_, workflow) =>
      val requester = sender()
      val dataFrame = timeFor(s"Processed sync. job request: $job") {
        QueryEngine.executeQuery(workflow, sqlContext, connectionHandler)
      }
      val result = timeFor(s"Unique query result processed.") {
        QueryResult.createQueryResult(
          toResultSet(dataFrame, toColumnMetadata(workflow)), 0, true)
      }
      requester ! result


    case job@PagedExecute(_, _, _, pageSize) =>
      timeFor(s"$me Processed paged job request : $job") {
        startNewJob(job, pageSize)
      }

    case job: AsyncExecute =>
      timeFor(s"$me Processed async. job request : $job") {
        startNewJob(job)
      }

    case ProcessNextChunk(queryId) if currentJob.exists(_.queryId == queryId) =>
      timeFor(s"$me Processed 'ProcessNextChunk' request (query: $queryId") {
        keepProcessingJob()
      }

    case Stop(queryId) if currentJob.exists(_.queryId == queryId) =>
      timeFor(s"$me Stopped request (query: $queryId") {
        stopCurrentJob()
      }

    case other => logger.error(s"[${other.getClass} Unhandled message : $other]")

  }

  //  Helpers

  def me: String = s"[QueryExecutor#${context.self}}]"

  /**
   * Start a new async query job. This will execute the given query
   * on SparkSQL and the repartition results for handling them in
   * smaller pieces called chunks. Chunk size should be small enough
   * in order to fit in driver memory.
   *
   * @param job Query to be asynchronously executed.
   */
  def startNewJob(job: AsyncJob, pageSize: Int = defaultChunkSize): Unit =
    timeFor(s"$me Job ${job.queryId} is started") {
      //  Update current job
      currentJob = Option(job)
      //  Create SchemaRDD from query
      val dataFrame = provider(job.workflow, connectionHandler, sqlContext)
      //  Update current schema
      currentSchema = Option(dataFrame.schema)
      if (asyncStoppable) {
        logger.debug(s"$me Processing ${job.queryId} as stoppable...")
        //  Split RDD into chunks of approx. 'defaultChunkSize' size
        dataFrame.rdd.countApprox(timeoutCountApprox.toMillis).onComplete { amount =>
          timeFor(s"$me Defined chunks (RDD size ~ $amount elements)...") {
            val repartitioned = dataFrame
              .repartition((amount.high / pageSize).toInt + {
              if (amount.high % pageSize == 0) 0 else 1
            }).rdd
            logger.debug(s"DataFrame split into ${repartitioned.partitions.length} partitions")
            rddChunks = repartitioned
              .toLocalIterator
              .grouped(pageSize)
              .map(_.iterator)
              .zipWithIndex
            if (repartitioned.partitions.length==0){
              val result = QueryResult.createQueryResult(
                toResultSet(List().toIterator, dataFrame.schema, toColumnMetadata(job.workflow)),
                0,
                true)
              result.setQueryId(job.queryId)
              job.resultHandler.processResult(result)
            }
          }
        }
      } else {
        timeFor(s"$me Processed ${job.queryId} as unstoppable...") {
          //  Prepare query as an only chunk, omitting stop messages
          rddChunks = List(dataFrame.rdd.toLocalIterator -> 0).iterator
        }
      }
      //  Begin processing current job
      keepProcessingJob()

    }

  /**
   * Process or set as finished current job if there are no chunks left.
   */
  def keepProcessingJob(): Unit =
    for {
      job <- currentJob
      schema <- currentSchema
    } {
      if (!rddChunks.hasNext) {
        logger.debug(s"$me Job ${job.queryId} has " +
          s"no chunks left to process")
        context.parent ! Finished(job.queryId)
      }
      else {
        logger.debug(s"$me Preparing to process " +
          s"next chunk of ${job.queryId}")
        val chunk = rddChunks.next()
        val isLast = !rddChunks.hasNext
        processChunk(chunk, isLast, schema)
        self ! ProcessNextChunk(job.queryId)
      }
    }

  /**
   * Process the given chunk as query result set.
   *
   * @param chunk The chunk to be processed
   * @param isLast Indicate the way to find out if given chunk is the last.
   */
  def processChunk(
    chunk: => Chunk,
    isLast: => Boolean,
    schema: StructType): Unit = {
    val (rows, idx) = chunk
    currentJob.foreach { job =>
      job.resultHandler.processResult {
        logger.debug(s"Preparing query result [$idx] for query ${job.queryId}")
        val result = QueryResult.createQueryResult(
          toResultSet(rows, schema, toColumnMetadata(job.workflow)),
          idx,
          isLast)
        result.setQueryId(job.queryId)
        logger.info(s"Query result [$idx] for query ${job.queryId} sent to resultHandler")
        result
      }

    }

  }

  /**
   * Stop processing current asynchronous query job.
   */
  def stopCurrentJob(): Unit = {
    currentJob = None
    currentSchema = None
    rddChunks = List().iterator
  }

}

object QueryExecutor {

  type DataFrameProvider = (LogicalWorkflow, ConnectionHandler, SparkSQLContext) => DataFrame

  def apply(
    sqlContext: SparkSQLContext,
    defaultChunkSize: Int,
    provider: DataFrameProvider,
    connectionHandler: ConnectionHandler,
    asyncStoppable: Boolean = true): Props =
    Props(new QueryExecutor(
      sqlContext,
      defaultChunkSize,
      provider,
      connectionHandler,
      asyncStoppable))

  case class ProcessNextChunk(queryId: QueryManager#QueryId)

}
