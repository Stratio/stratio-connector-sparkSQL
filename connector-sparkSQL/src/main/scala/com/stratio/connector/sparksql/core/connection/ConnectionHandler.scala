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
package com.stratio.connector.sparksql.core.connection

import com.stratio.connector.commons.Loggable
import com.stratio.connector.sparksql.core.providerConfig.{Constants, providers}
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.apache.spark.sql.SQLContext

class ConnectionHandler extends Loggable with Constants{

  type ConnectionId = String

  private var connections: Map[ConnectionId, Connection] = Map()

  private def withConnections[T](f: => T): T = {
    connections.synchronized(f)
  }

  def createConnection(
    config: ConnectorClusterConfig,
    sqlContext: SQLContext,
    credentials: Option[ICredentials] = None): Option[ConnectionId] = {
    val connectionId = config.getName.getName
    withConnections {
      if (connections.isDefinedAt(connectionId)) {
        logger.error(s"Connection [$connectionId] already exists")
        None
      }
      else {
        val dataStoreName = config.getDataStoreName.getName
        logger.info(s"Datastore name: $dataStoreName")
        providers.apply(config.getDataStoreName.getName).map{ provider =>
          val connection = provider.createConnection(config,sqlContext,credentials)
          connections += (connectionId -> connection)
          logger.info(s"Connected to [$connectionId]")
          connectionId
        }
      }
    }
  }

  def closeConnection(connection: ConnectionId): Unit = {
    withConnections{
      connections -= connection
    }
    logger.info(s"Disconnected from [$connection]")
  }

  def isConnected(connection: ConnectionId): Boolean = {
    withConnections{
      connections.isDefinedAt(connection)
    }
  }

  def getConnection(connection: ConnectionId): Option[Connection] = {
    withConnections{
      connections.get(connection)
    }
  }

  def getConnections(): Map[ConnectionId, Connection] =
    withConnections(connections)

  def startJob(connectionId: ConnectionId): Unit = {
    withConnections{
      connections.get(connectionId).foreach{connection =>
        connections += (connectionId -> connection.setBusy(true))
        logger.info(s"A new job has started in cluster [$connectionId]")
      }
    }
  }

  def endJob(connectionId: ConnectionId): Unit = {
    withConnections{
      connections.get(connectionId).foreach{connection =>
        connections += (connectionId -> connection.setBusy(false))
        logger.info(s"A new job has finished in cluster [$connectionId]")
      }
    }
  }

  def closeAllConnections(): Unit = {
    withConnections{
      connections = Map()
    }
  }

}
