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

import com.stratio.crossdata.common.result.QueryResult
import com.stratio.connector.commons.timer
import org.apache.spark.sql.types.StructType
import scala.concurrent.duration._
import akka.actor.{Props, Actor}
import com.stratio.connector.sparksql.{Metrics, Loggable, SparkSQLConnector, SparkSQLContext}
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
  asyncStoppable: Boolean = true) extends Actor
with Loggable
with Metrics {

  import SparkSQLConnector._
  import QueryManager._
  import timer._

  type Chunk = (Iterator[Row], Int)

  var currentJob: Option[JobCommand] = None

  var currentSchema: Option[StructType] = None

  var rddChunks: Iterator[Chunk] = List.empty[Chunk].iterator

  val timeoutCountApprox = connectorConfig.get.getInt(CountApproxTimeout).seconds

  private val me: String = s"[QueryExecutor#${context.self}}]"

  override def receive = {

    case job: JobCommand =>
      timeFor(s"$me Processing job request : $job") {
        startNewJob(job)
      }

    case ProcessNextChunk(queryId) if currentJob.exists(_.queryId == queryId) =>
      timeFor(s"$me Processing 'ProcessNextChunk' request (query: $queryId") {
        keepProcessingJob()
      }

    case Stop(queryId) if currentJob.exists(_.queryId == queryId) =>
      timeFor(s"$me Stopping request (query: $queryId") {
        stopCurrentJob()
      }

  }

  //  Helpers

  /**
   * Start a new async query job. This will execute the given query
   * on SparkSQL and the repartition results for handling them in
   * smaller pieces called chunks. Chunk size should be small enough
   * in order to fit in driver memory.
   *
   * @param job Query to be asynchronously executed.
   */
  def startNewJob(job: JobCommand): Unit =
    timeFor(s"$me Starting job ${job.queryId}") {
      //  Update current job
      currentJob = Option(job)
      //  Create SchemaRDD from query
      val dataFrame = provider(job.workflow, sqlContext)
      //  Update current schema
      currentSchema = Option(dataFrame.schema)
      if (asyncStoppable) {
        logger.debug(s"$me Processing ${job.queryId} as stoppable...")
        //  Split RDD into chunks of approx. 'defaultChunkSize' size
        dataFrame.rdd.countApprox(timeoutCountApprox.toMillis).onComplete { amount =>
          timeFor(s"$me Defining chunks (RDD size ~ $amount elements)...") {
            rddChunks = dataFrame
              .repartition((amount.high / defaultChunkSize).toInt)
              .rdd
              .toLocalIterator
              .grouped(defaultChunkSize)
              .map(_.iterator)
              .zipWithIndex
          }
        }
      } else {
        timeFor(s"$me Processing ${job.queryId} as unstoppable...") {
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
        processChunk(rddChunks.next(), rddChunks.hasNext, schema)
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
      QueryResult.createQueryResult(
        toResultSet(rows, schema, toColumnMetadata(job.workflow)),
        idx,
        isLast)
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

  type DataFrameProvider = (LogicalWorkflow, SparkSQLContext) => DataFrame

  def apply(
    sqlContext: SparkSQLContext,
    defaultChunkSize: Int,
    provider: DataFrameProvider,
    asyncStoppable: Boolean = true): Props =
    Props(new QueryExecutor(
      sqlContext,
      defaultChunkSize,
      provider,
      asyncStoppable))

  case class ProcessNextChunk(queryId: QueryManager#QueryId)

}
