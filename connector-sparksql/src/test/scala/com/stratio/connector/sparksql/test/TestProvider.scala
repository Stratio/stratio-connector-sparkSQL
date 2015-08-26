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
package com.stratio.connector.sparksql.test

import com.stratio.connector.sparksql.core.Provider
import com.stratio.connector.sparksql.core.connection.Connection
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

case object TestProvider extends Provider with TestProviderConstants{

  override val dataSource: String = "org.apache.spark.sql.test"

  override val manifest: String = "TestDataStore.xml"

  override val name: String = "Test"




  val DefaultPort = "9160"

  override def createConnection(
    config: ConnectorClusterConfig,
    sqlContext: SQLContext,
    credentials: Option[ICredentials]): Connection = {


    println("Connection created")

    new Connection(config, credentials)
  }
}
