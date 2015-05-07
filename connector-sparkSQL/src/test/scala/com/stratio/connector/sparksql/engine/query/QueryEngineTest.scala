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

import akka.testkit.TestProbe
import com.stratio.connector.sparksql.`package`.SparkSQLContext
import com.stratio.connector.sparksql.connection.ConnectionHandler
import com.stratio.connector.sparksql.engine.query.QueryManager.{Stop, PagedExecute, AsyncExecute}
import com.stratio.connector.sparksql.Test
import com.stratio.connector.sparksql.providers.Provider
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.data.{ColumnName, TableName, ClusterName}
import com.stratio.crossdata.common.logicalplan.{Select, LogicalStep, LogicalWorkflow}
import com.stratio.crossdata.common.metadata.{DataType, ColumnType, Operations, ColumnMetadata}
import com.stratio.crossdata.common.statements.structures.{AsteriskSelector, Selector}

class QueryEngineTest extends Test("QueryEngine") {

  import scala.collection.JavaConversions._

  val sqlContext = None.orNull[SparkSQLContext]
  val workflow = new LogicalWorkflow(List())
  val resultHandler = None.orNull[IResultHandler]
  val connectionHandler = None.orNull[ConnectionHandler]
  val provider = new Provider {
    val datasource = "my-provider"
  }

  it should "execute async. queries" in {
    val queryManager = TestProbe()
    val qe = new QueryEngine(sqlContext, queryManager.ref, connectionHandler)
    qe.asyncExecute("q1", workflow, resultHandler)
    queryManager.expectMsg(AsyncExecute("q1", workflow, resultHandler))
  }

  it should "execute paged queries" in {
    val pageSize = 0
    val queryManager = TestProbe()
    val qe = new QueryEngine(sqlContext, queryManager.ref, connectionHandler)
    qe.pagedExecute("q1", workflow, resultHandler, pageSize)
    queryManager.expectMsg(PagedExecute("q1", workflow, resultHandler, pageSize))
  }

  it should "stop in-progress queries" in {
    val queryManager = TestProbe()
    val qe = new QueryEngine(sqlContext, queryManager.ref, connectionHandler)
    qe.stop("q1")
    queryManager.expectMsg(Stop("q1"))
  }

  it should "get metadata from workflow (with no last step in workflow)" in {
    val wf = new LogicalWorkflow(List())
    QueryEngine.toColumnMetadata(wf) should equal(List.empty[ColumnMetadata])
  }

  it should "get metadata from workflow" in {
    val selector = {
      val s = new AsteriskSelector(new TableName("catalog", "table"))
      s.setAlias("field1")
      s
    }
    val lastStep: LogicalStep = new Select(
      Set[Operations](),
      Map[Selector, String](
        selector -> "field1"),
      Map[String, ColumnType](
        "field1" -> new ColumnType(DataType.TEXT)),
      Map[Selector, ColumnType]()
    )
    val wf = new LogicalWorkflow(List(), lastStep, 0)
    val metadata = QueryEngine.toColumnMetadata(wf)
    val expectedMetadata = List(
      new ColumnMetadata(
      {
        val name = new ColumnName("catalog", "table", "field1")
        name.setAlias("field1")
        name
      },
      Array[AnyRef](),
      new ColumnType(DataType.TEXT)))
    metadata.head.getColumnType should equal(expectedMetadata.head.getColumnType)
    metadata.head.getName should equal(expectedMetadata.head.getName)
    metadata.head.getParameters should equal(expectedMetadata.head.getParameters)
  }


  val queryTranslation = Map(s"""
                    |SELECT catalog.table.field1, catalog.table.field2
                    |FROM catalog.table;""" ->
                  s"""
                      |SELECT table.field1, table.field2
                      |FROM table;""",
    s"""
       |select OL.ol_w_id,OL.ol_d_id,OL.ol_o_id,AVG_Amoun,avg(OL.ol_amount) as average
       |from (select d_id,d_w_id, avg(ol_amount) as AVG_Amoun
       |from district D, tpcc.order_line OL_A
       |where D.d_id=OL_A.ol_d_id and D.d_w_id=OL_A.ol_w_id and d_id=3 and d_w_id=241 group by d_id,d_w_id ) A, tpcc.order_line OL
       |where A.d_id=OL.ol_d_id and A.d_w_id=OL.ol_w_id and OL.ol_d_id=3 and OL.ol_w_id=241
       |group by OL.ol_w_id,OL.ol_d_id,OL.ol_o_id,AVG_Amoun having avg(OL.ol_amount) > AVG_Amoun order by average desc""" ->
      s"""
         |select OL.ol_w_id,OL.ol_d_id,OL.ol_o_id,AVG_Amoun,avg(OL.ol_amount) as average
         |from (select d_id,d_w_id, avg(ol_amount) as AVG_Amoun
         |from district D, order_line OL_A
         |where D.d_id=OL_A.ol_d_id and D.d_w_id=OL_A.ol_w_id and d_id=3 and d_w_id=241 group by d_id,d_w_id ) A, order_line OL
         |where A.d_id=OL.ol_d_id and A.d_w_id=OL.ol_w_id and OL.ol_d_id=3 and OL.ol_w_id=241
         |group by OL.ol_w_id,OL.ol_d_id,OL.ol_o_id,AVG_Amoun having avg(OL.ol_amount) > AVG_Amoun order by average desc""",
  s"""select catalog.catalog.catalog as column, catalog.catalog.other as catalog, precatalog.table.column as other from catalog.catalog where catalog.catalog.catalog = 10""" ->
    s"""select catalog.catalog as column, catalog.other as catalog, table.column as other from catalog where catalog.catalog = 10""",
  s"""select catalog.table.column1 from table where column2 = \" catalog.\""""" ->
  s"""select table.column1 from table where column2 = \" catalog.\""""",
    s"""select catalog.table.column1 from table where column2 = ' catalog.'"""" ->
      s"""select table.column1 from table where column2 = ' catalog.'"""",
  s"""select catalog.table.column1 from table where column2 = ' The catalog. name'"""" ->
    s"""select table.column1 from table where column2 = ' The catalog. name'"""",
    s"""select catalog.table.column1 from table where column2 = \" The catalog. name\""""" ->
      s"""select table.column1 from table where column2 = \" The catalog. name\"""""

  )

  it should "format Crossdata query for adapting it to SparkSQL format" in {

    queryTranslation.foreach{
      case(k,v)=>  QueryEngine.sparkSQLFormat(k.stripMargin,List("tpcc","catalog", "precatalog")) should equal(v.stripMargin)
    }

  }

  it should "begin and end a job in connectionHandler when using 'withClusters'" in {
    val ch = new ConnectionHandler {
      type EnableAction = (ConnectionId, Boolean)
      var execution: List[EnableAction] = List()

      override def startJob(connectionId: ConnectionId): Unit = {
        execution = (connectionId -> true) +: execution
      }

      override def endJob(connectionId: ConnectionId): Unit = {
        execution = (connectionId -> false) +: execution
      }
    }
    val cluster = "Cluster1"
    QueryEngine.withClusters(ch, List(new ClusterName(cluster)))(clusters =>())
    ch.execution.reverse should equal(List(cluster -> true, cluster -> false))
  }

}
