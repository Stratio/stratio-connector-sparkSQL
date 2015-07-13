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

import akka.testkit.{TestProbe, TestActorRef}
import com.stratio.connector.sparksql.Test
import com.stratio.connector.sparksql.core.providerConfig.`package`.SparkSQLContext
import com.stratio.connector.sparksql.core.providerConfig.providers
import com.stratio.connector.sparksql.core.connection.ConnectionHandler
import com.stratio.connector.sparksql.core.engine.query.QueryExecutor.DataFrameProvider
import com.stratio.connector.sparksql.core.engine.query.QueryManager.{Finished, Stop, AsyncExecute}
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryManagerTest extends Test("QueryManager") {

  val fakeSqlContext = None.orNull[SparkSQLContext]
  val fakeConnectionHandler = None.orNull[ConnectionHandler]
  val fakeDataFrameProvider = None.orNull[DataFrameProvider]
  val fakeWorkflow = None.orNull[LogicalWorkflow]
  val fakeResultHandler = None.orNull[IResultHandler]

  val executorAmount = 3

  it should "initialize properly" in {

    val fakeExecutors = (1 to executorAmount).map(_ => TestProbe())
    val executorRefs = fakeExecutors.map(_.ref).toSet

    val qm = TestActorRef(new QueryManager(
      executorsAmount = executorAmount,
      sqlContext = fakeSqlContext,
      connectionHandler = fakeConnectionHandler,
      provider = fakeDataFrameProvider) {
      freeExecutors = executorRefs
    })

    val state = qm.underlyingActor
    state.busy should equal(false)
    state.freeExecutors should equal(executorRefs)
    state.pendingQueries.isEmpty should equal(true)

  }

  it should "assign a job to any free executor" in {

    val fakeExecutors = (1 to executorAmount).map(_ => TestProbe())
    val executorRefs = fakeExecutors.map(_.ref).toSet

    val qm = TestActorRef(new QueryManager(
      executorsAmount = executorAmount,
      sqlContext = fakeSqlContext,
      connectionHandler = fakeConnectionHandler,
      provider = fakeDataFrameProvider) {
      freeExecutors = executorRefs
    })

    qm ! AsyncExecute("q1",fakeWorkflow,fakeResultHandler)

    val state = qm.underlyingActor

    state.busy should equal(false)
    executorRefs.contains(state.pendingQueries.head._2) should equal(true)
    state.pendingQueries.size should equal(1)
    state.freeExecutors.forall(e => executorRefs.contains(e)) should equal(true)

  }

  it should "stop any in-progress job" in {

    val fakeExecutors = (1 to executorAmount).map(_ => TestProbe())
    val executorRefs = fakeExecutors.map(_.ref).toSet

    val qm = TestActorRef(new QueryManager(
      executorsAmount = executorAmount,
      sqlContext = fakeSqlContext,
      connectionHandler = fakeConnectionHandler,
      provider = fakeDataFrameProvider) {
      freeExecutors = executorRefs
    })

    qm ! AsyncExecute("q1",fakeWorkflow,fakeResultHandler)
    val state1 = qm.underlyingActor
    val Some(busyExecutor) =
      fakeExecutors.find(tp => tp.ref == state1.pendingQueries("q1"))
    qm ! Stop("q1")
    val Seq(AsyncExecute("q1",_,_),Stop("q1")) = busyExecutor.receiveN(2)
    val state2 = qm.underlyingActor

    state2.busy should equal(false)
    state2.freeExecutors should equal(executorRefs)
    state2.pendingQueries.isEmpty should equal(true)

  }

  it should "stash any incoming job request and un-stash " +
    "when any executor becomes free" in {

    val fakeExecutors = (1 to executorAmount).map(_ => TestProbe())
    val executorRefs = fakeExecutors.map(_.ref).toSet

    val qm = TestActorRef(new QueryManager(
      executorsAmount = executorAmount,
      sqlContext = fakeSqlContext,
      connectionHandler = fakeConnectionHandler,
      provider = fakeDataFrameProvider) {
      freeExecutors = executorRefs
    })

    (1 to (executorAmount + 1)).foreach(n =>
      qm ! AsyncExecute(s"q$n",fakeWorkflow,fakeResultHandler))

    val state1 = qm.underlyingActor

    state1.busy should equal(true)
    state1.pendingQueries.size should equal(executorAmount)
    state1.freeExecutors.size should equal(0)

    (1 to executorAmount).foreach(n =>
      qm ! Finished(s"q$n"))

    val state2 = qm.underlyingActor
    state1.busy should equal(false)
    state1.pendingQueries.size should equal(1)
    state1.freeExecutors.size should equal(executorAmount -1)

  }

}
