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

package com.stratio.connector.sparksql

import com.codahale.metrics.MetricRegistry
import com.stratio.crossdata.common.exceptions.InitializationException
import com.stratio.crossdata.common.utils.{Metrics => XDMetrics}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.xml.{Elem, XML}

object `package` {

  type SparkSQLContext = SQLContext with Catalog

}

private[sparksql] trait Constants {
  val ActorSystemName = "SparkSQLConnectorSystem"
  val ConfigurationFileConstant = "connector-application.conf"
  val SparkMaster: String = "spark.master"
  val SparkHome: String = "spark.home"
  val SparkDriverMemory = "spark.driver.memory"
  val SparkExecutorMemory = "spark.executor.memory"
  val SparkTaskCPUs = "spark.task.cpus"
  val SparkJars: String = "jars"
  val MethodNotSupported: String = "Not supported yet"
  val SparkSQLConnectorJobConstant: String = "SparkSQLConnectorJob"
  val Spark: String = "spark"
  val ConnectorConfigFile = "SparkSQLConnector.xml"
  val ConnectorName = "ConnectorName"
  val DataStoreName = "DataStoreName"
  val SQLContext = "SQLContext"
  val HIVEContext = "HiveContext"
  val CountApproxTimeout = "connector.count-approx-timeout"
  val QueryExecutorsAmount = "connector.query-executors.size"
  val ConnectorProvider = "connector.provider"
  val SQLContextType = "connector.sql-context-type"
  val AsyncStoppable = "connector.async-stoppable"
  val ChunkSize = "connector.query-executors.chunk-size"
  val CatalogTableSeparator = "_"
}

/**
 * Provides an accessor to SQLContext catalog.
 */
trait Catalog {
  _: SQLContext =>

  def getCatalog = catalog

}

/**
 * It provides an implicit metric registry.
 */
trait Metrics {

  implicit lazy val metrics: MetricRegistry = XDMetrics.getRegistry

}

/**
 * It provides a simple pretty logger.
 */
trait Loggable {

  implicit lazy val logger = LoggerFactory.getLogger(getClass)

}

/**
 * Configuration stuff related to SparkSQLConnector.
 */
trait Configuration {
  _: Loggable =>

  import SparkSQLConnector._

  //  References to 'connector-application'
  val connectorConfig: Try[Config] = {
    val input = Option(getClass.getClassLoader.getResourceAsStream(
      SparkSQLConnector.ConfigurationFileConstant))
    Try(input.fold {
      val message = s"Sorry, unable to find [${
        SparkSQLConnector.ConfigurationFileConstant
      }]"
      logger.error(message)
      throw new InitializationException(message)
    }(_ => ConfigFactory.load(SparkSQLConnector.ConfigurationFileConstant)))
  }

  //  References to 'SparkSQLConnector'
  val connectorConfigFile: Try[Elem] =
    Try(XML.load(
      getClass.getClassLoader.getResourceAsStream(ConnectorConfigFile)))

}
