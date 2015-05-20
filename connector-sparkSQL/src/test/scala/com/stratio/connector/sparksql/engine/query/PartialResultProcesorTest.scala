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

import java.util

import akka.testkit.TestProbe
import com.stratio.connector.sparksql.Test
import com.stratio.connector.sparksql.`package`.SparkSQLContext
import com.stratio.connector.sparksql.connection.ConnectionHandler
import com.stratio.connector.sparksql.engine.query.QueryManager.{AsyncExecute, PagedExecute, Stop}
import com.stratio.connector.sparksql.providers.Provider
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.data.{ClusterName, ColumnName, TableName}
import com.stratio.crossdata.common.logicalplan._
import com.stratio.crossdata.common.metadata.{ColumnMetadata, ColumnType, DataType, Operations}
import com.stratio.crossdata.common.statements.structures.{AsteriskSelector, Selector}
import org.scalatest._

class PartialResultProcesorTest extends FlatSpec with Matchers {

  behavior of "PartialResultProcessor"

  import scala.collection.JavaConversions._

  val sqlContext = None.orNull[SparkSQLContext]
  val workflow = new LogicalWorkflow(List())
  val resultHandler = None.orNull[IResultHandler]
  val connectionHandler = None.orNull[ConnectionHandler]
  val provider = new Provider {
    val datasource = "my-provider"
  }



  def CATALOG= "CATALOG"
  def LEFT_TABLE_NAME = "left_table_name"
  def RIGHT_TABLE_NAME = "right_table_name"
  def CLUSTER_NAME = "clusterName"




  it should "return the partial result in a logical workflow with crossJoin" in {

    val project = new Project(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS), new TableName(CATALOG, LEFT_TABLE_NAME), new ClusterName(CLUSTER_NAME))
    val partialResult: LogicalStep = new PartialResults(Set(Operations.PARTIAL_RESULTS))
    val join = new Join(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS),"id")
    join.setPreviousSteps(List(partialResult))
    project.setNextStep(join)

    join.setNextStep(new Select(Set(Operations.SELECT_OPERATOR),Map.empty[Selector,String],Map.empty[String,ColumnType],Map.empty[Selector,ColumnType]))

    val initialSteps = List(project/*,rightJoin*/)

    val  lgfw = new LogicalWorkflow(initialSteps)

    val partialResults: List[PartialResults] =  PartialResultProcessor().recoveredPartialResult(lgfw).toList

    partialResults.size should be(1)
    partialResults.foreach(_.isInstanceOf[PartialResults] should equal(true))

  }



  it should "return the partial result in a logical workflow with full outer" in {


    val project = new Project(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS), new TableName(CATALOG, LEFT_TABLE_NAME), new ClusterName(CLUSTER_NAME))
    val partialResult: LogicalStep = new PartialResults(Set(Operations.PARTIAL_RESULTS))
    val join = new Join(Set(Operations.SELECT_FULL_OUTER_JOIN_PARTIALS_RESULTS),"id")
    join.setPreviousSteps(List(partialResult))
    project.setNextStep(join)

    join.setNextStep(new Select(Set(Operations.SELECT_OPERATOR),Map.empty[Selector,String],Map.empty[String,ColumnType],Map.empty[Selector,ColumnType]))

    val initialSteps = List(project/*,rightJoin*/)

    val  lgfw = new LogicalWorkflow(initialSteps)

    val partialResults: List[PartialResults] =  PartialResultProcessor().recoveredPartialResult(lgfw).toList

    partialResults.size should be(1)
    partialResults.foreach(_.isInstanceOf[PartialResults] should equal(true))

  }


  it should "return the partial result in a logical workflow with inner join" in {


    val project = new Project(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS), new TableName(CATALOG, LEFT_TABLE_NAME), new ClusterName(CLUSTER_NAME))
    val partialResult: LogicalStep = new PartialResults(Set(Operations.PARTIAL_RESULTS))
    val join = new Join(Set(Operations.SELECT_INNER_JOIN_PARTIALS_RESULTS),"id")
    join.setPreviousSteps(List(partialResult))
    project.setNextStep(join)

    join.setNextStep(new Select(Set(Operations.SELECT_OPERATOR),Map.empty[Selector,String],Map.empty[String,ColumnType],Map.empty[Selector,ColumnType]))

    val initialSteps = List(project/*,rightJoin*/)

    val  lgfw = new LogicalWorkflow(initialSteps)

    val partialResults: List[PartialResults] =  PartialResultProcessor().recoveredPartialResult(lgfw).toList

    partialResults.size should be(1)
    partialResults.foreach(_.isInstanceOf[PartialResults] should equal(true))

  }


  it should "return the partial result in a logical workflow with left outer" in {


    val project = new Project(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS), new TableName(CATALOG, LEFT_TABLE_NAME), new ClusterName(CLUSTER_NAME))
    val partialResult: LogicalStep = new PartialResults(Set(Operations.PARTIAL_RESULTS))
    val join = new Join(Set(Operations.SELECT_LEFT_OUTER_JOIN_PARTIALS_RESULTS),"id")
    join.setPreviousSteps(List(partialResult))
    project.setNextStep(join)

    join.setNextStep(new Select(Set(Operations.SELECT_OPERATOR),Map.empty[Selector,String],Map.empty[String,ColumnType],Map.empty[Selector,ColumnType]))

    val initialSteps = List(project/*,rightJoin*/)

    val  lgfw = new LogicalWorkflow(initialSteps)

    val partialResults: List[PartialResults] =  PartialResultProcessor().recoveredPartialResult(lgfw).toList

    partialResults.size should be(1)
    partialResults.foreach(_.isInstanceOf[PartialResults] should equal(true))

  }


  it should "return the partial result in a logical workflow with right outer" in {


    val project = new Project(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS), new TableName(CATALOG, LEFT_TABLE_NAME), new ClusterName(CLUSTER_NAME))
    val partialResult: LogicalStep = new PartialResults(Set(Operations.PARTIAL_RESULTS))
    val join = new Join(Set(Operations.SELECT_RIGHT_OUTER_JOIN_PARTIALS_RESULTS),"id")
    join.setPreviousSteps(List(partialResult))
    project.setNextStep(join)

    join.setNextStep(new Select(Set(Operations.SELECT_OPERATOR),Map.empty[Selector,String],Map.empty[String,ColumnType],Map.empty[Selector,ColumnType]))

    val initialSteps = List(project/*,rightJoin*/)

    val  lgfw = new LogicalWorkflow(initialSteps)

    val partialResults: List[PartialResults] =  PartialResultProcessor().recoveredPartialResult(lgfw).toList

    partialResults.size should be(1)
    partialResults.foreach(_.isInstanceOf[PartialResults] should equal(true))

  }


  it should "return the partial result in a logical workflow with two partial result" in {


    val project = new Project(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS), new TableName(CATALOG, LEFT_TABLE_NAME), new ClusterName(CLUSTER_NAME))
    val partialResult1: LogicalStep = new PartialResults(Set(Operations.PARTIAL_RESULTS))
    val join = new Join(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS),"id")
    join.setPreviousSteps(List(partialResult1))
    project.setNextStep(join)


    val partialResult2: LogicalStep = new PartialResults(Set(Operations.PARTIAL_RESULTS))

    val join2 = new Join(Set(Operations.SELECT_CROSS_JOIN_PARTIALS_RESULTS),"id 2")
    join2.setPreviousSteps(List(partialResult2))
    join.setNextStep(join2)


    join2.setNextStep(new Select(Set(Operations.SELECT_OPERATOR),Map.empty[Selector,String],Map.empty[String,ColumnType],Map.empty[Selector,ColumnType]))

    val initialSteps = List(project/*,rightJoin*/)

    val  lgfw = new LogicalWorkflow(initialSteps)

    val partialResults: List[PartialResults] =  PartialResultProcessor().recoveredPartialResult(lgfw).toList

    partialResults.size should be(2)
    partialResults.foreach(_.isInstanceOf[PartialResults] should equal(true))

  }


}
