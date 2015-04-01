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
package com.stratio.connector.sparksql.connection

import com.stratio.connector.sparksql.UnitTest

import scala.language.implicitConversions
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.data.ClusterName

class ConnectionHandlerTest extends UnitTest("ConnectionHandler")
with ConnectionHandlerSampleValues {

  it should "add a new connection" in {
    val ch = new ConnectionHandler
    ch.getConnection("cluster-1").isDefined should equal(false)
    val Some(connectionId) = ch.createConnection(clusterConfig1)
    val retrieved = ch.getConnection(connectionId).get
    retrieved.config should equal(clusterConfig1)
    retrieved.credentials should equal(None)
    retrieved.busy should equal(false)
  }

  it should "fail adding a new connection when it already exists" in {
    val ch = new ConnectionHandler
    ch.getConnection("cluster-1").isDefined should equal(false)
    val Some(_) = ch.createConnection(clusterConfig1)
    ch.createConnection(clusterConfig1).isDefined should equal(false)
  }

  it should "close an existing connection" in {
    val ch = new ConnectionHandler
    val Some(connection) = ch.createConnection(clusterConfig1)
    ch.getConnection(connection).isDefined should equal(true)
    ch.closeConnection(connection)
    ch.getConnection(connection).isDefined should equal(false)
  }

  it should "not fail when closing a non existing connection" in {
    val ch = new ConnectionHandler
    try {
      ch.closeConnection("non-existing-connection")
    } catch {
      case _: Throwable => fail()
    }
  }

  it should "check whether a connection has been added or not" in {
    val ch = new ConnectionHandler
    ch.isConnected("non-existing-connection") should equal(false)
    val Some(connection) = ch.createConnection(clusterConfig1)
    ch.isConnected(connection) should equal(true)
    ch.closeConnection(connection)
    ch.isConnected(connection) should equal(false)
  }

  it should "set a connection as busy when a job is started" in {
    val ch = new ConnectionHandler
    val Some(connection) = ch.createConnection(clusterConfig1)
    ch.getConnection(connection).exists(_.busy == false) should equal(true)
    ch.startJob(connection)
    ch.getConnection(connection).exists(_.busy == true) should equal(true)
  }

  it should "not fail while setting a connection as busy " +
    "when a job is started and connection does not exist" in {
    val ch = new ConnectionHandler
    try {
      ch.startJob("non-existent-connection")
    } catch {
      case _: Throwable => fail()
    }
  }

  it should "set a connection as not busy when a job is started" in {
    val ch = new ConnectionHandler
    val Some(connection) = ch.createConnection(clusterConfig1)
    ch.getConnection(connection).exists(_.busy == false) should equal(true)
    ch.startJob(connection)
    ch.getConnection(connection).exists(_.busy == true) should equal(true)
    ch.endJob(connection)
    ch.getConnection(connection).exists(_.busy == false) should equal(true)
  }

  it should "not fail while setting a connection as not busy " +
    "when a job is started and connection does not exist" in {
    val ch = new ConnectionHandler
    try {
      ch.endJob("non-existent-connection")
    } catch {
      case _: Throwable => fail()
    }
  }

}

trait ConnectionHandlerSampleValues {

  import scala.collection.JavaConversions._

  val clusterConfig1 =
    new ConnectorClusterConfig(
      "cluster-1",
      Map[String, String](),
      Map[String, String]())

  implicit def toClusterName(name: String): ClusterName =
    new ClusterName(name)
}