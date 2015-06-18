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

import akka.actor.{Actor, ActorRef, Props}
import com.stratio.connector.sparksql.connection.ConnectionHandler
import com.stratio.connector.sparksql.engine.query.QueryExecutor.DataFrameProvider
import com.stratio.connector.sparksql.engine.query.QueryManager.{Finished, AsyncExecute}
import com.stratio.connector.sparksql.{Catalog, Test}
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.exceptions.ExecutionException
import com.stratio.crossdata.common.logicalplan.{Project, LogicalStep, LogicalWorkflow}
import com.stratio.crossdata.common.metadata.Operations
import com.stratio.crossdata.common.result.QueryResult
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._
import com.stratio.crossdata.common.data.{Row => XDRow, ClusterName, TableName}
@RunWith(classOf[JUnitRunner])
class QueryExecutorTest extends Test("QueryExecutor") with Serializable {test =>

  //  Prepare workspace properties

  System.setProperty("hive.metastore.warehouse.dir","/tmp/warehouse")

  //  Initialize vars...

  val sqlContext = new HiveContext(Test.sparkContext) with Catalog
  val connectionHandler = new ConnectionHandler
  val provider: DataFrameProvider = (workflow, connectionHandler, sqlContext) =>
  QueryEngine.executeQuery(
        workflow,
        sqlContext,
        connectionHandler)

  //  Populate table ...


  def generate(n: Int)(sqlContext: SQLContext): DataFrame = {
    val rdd = Test.sparkContext.parallelize(for {
        id <- 1 to n
    } yield Student(id, s"Name$id"), 4)
    sqlContext.createDataFrame(rdd)
  }

  val amount = 150

  generate(amount)(sqlContext).saveAsTable(
  "students",
  "org.apache.spark.sql.parquet",
      SaveMode.Overwrite,
      Map("path" -> "/tmp/students"))


  //  Test definition ...

  val (queryId,query) =
    "query-1" -> "SELECT catalog.students.id, catalog.students.name FROM catalog.students"


  it should "execute an async stopable job with same-sized chunks" in {

    val defaultChunkSize = amount / 3

    val lw = new LogicalWorkflow(List(
        new Project(
          Set[Operations](),
          new TableName("catalog","table"),
          new ClusterName("cluster1"))
      )
    )
    lw.setSqlDirectQuery(query)
    val executor = system.actorOf(Props(new Actor {
      val slave = context.actorOf(Props(new QueryExecutor(
        sqlContext,
        defaultChunkSize,
        provider,
        connectionHandler,
        asyncStoppable = true)))
      slave ! AsyncExecute(
        queryId,
        lw,
        resultHandler(self))
      def receive = {
        case msg => test.self forward msg
      }
    }))
    receiveN(amount).forall(_.isInstanceOf[XDRow])
    expectMsg(Finished(queryId))

  }

  it should "execute an async stopable job with distinct-sized chunks" in {

    val defaultChunkSize = amount / 4

    val lw = new LogicalWorkflow(List(
      new Project(
        Set[Operations](),
        new TableName("catalog","table"),
        new ClusterName("cluster1"))
    ))
    lw.setSqlDirectQuery(query)
    val executor = system.actorOf(Props(new Actor {
      val slave = context.actorOf(Props(new QueryExecutor(
        sqlContext,
        defaultChunkSize,
        provider,
        connectionHandler,
        asyncStoppable = true)))
      slave ! AsyncExecute(
        queryId,
        lw,
        resultHandler(self))
      def receive = {
        case msg => test.self forward msg
      }
    }))
    receiveN(amount).forall(_.isInstanceOf[XDRow])
    expectMsg(Finished(queryId))

  }

  it should "execute an async non-stopable job with same result as an stoppable one" in {

    val defaultChunkSize = amount / 3

    val lw = new LogicalWorkflow(List(
      new Project(
        Set[Operations](),
        new TableName("catalog","table"),
        new ClusterName("cluster1"))
    ))
    lw.setSqlDirectQuery(query)
    val executor = system.actorOf(Props(new Actor {
      val slave = context.actorOf(Props(new QueryExecutor(
        sqlContext,
        defaultChunkSize,
        provider,
        connectionHandler,
        asyncStoppable = false)))
      slave ! AsyncExecute(
        queryId,
        lw,
        resultHandler(self))
      def receive = {
        case msg => test.self forward msg
      }
    }))
    receiveN(amount).forall(_.isInstanceOf[XDRow])
    expectMsg(Finished(queryId))

  }

  //  Helpers

  def resultHandler(responseTo: ActorRef) = new IResultHandler {
    override def processException(
      queryId: String,
      exception: ExecutionException): Unit = fail(exception)

    override def processResult(result: QueryResult): Unit = {
      result.getResultSet.iterator.foreach(responseTo ! _)
    }
  }

}

case class Student(id: Int, name: String)
