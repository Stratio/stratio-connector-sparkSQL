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

import akka.actor.{Kill, ActorRef, ActorRefFactory, ActorSystem}
import com.stratio.connector.sparksql.engine.SparkSQLMetadataListener
import com.stratio.connector.sparksql.engine.query.{QueryManager, QueryEngine}
import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.common.data.ClusterName
import com.stratio.crossdata.common.exceptions.{InitializationException, UnsupportedException}
import com.stratio.crossdata.common.security.ICredentials
import com.stratio.crossdata.connectors.ConnectorApp
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import com.stratio.connector.commons.timer

import scala.xml.XML

class SparkSQLConnector(system: ActorRefFactory) extends IConnector
with Loggable {

  import timer._
  import SparkSQLConnector._

  private var sparkContext: Option[SparkContext] = None

  private var sqlContext: Option[SparkSQLContext] = None

  private var clusterConfig: Option[ConnectorClusterConfig] = None

  private var queryManager: Option[ActorRef] = None

  //  Config parameters

  val queryExecutors: Int =
    connectorConfig.getInt(QueryExecutorsAmount)

  val provider: Provider =
    providers(connectorConfig.getString(ConnectorProvider))

  val sqlContextType: String =
    connectorConfig.getString(SQLContextType)

  //  IConnector implemented methods

  override def getConnectorName: String =
    connectorConfigFile.child
      .find(_.label == ConnectorName)
      .map(_.text)
      .getOrElse(
        throw new NoSuchElementException(
          s"Property $ConnectorName was not set"))

  override def getDatastoreName: Array[String] =
    connectorConfigFile.child
      .find(_.label == DataStoreName)
      .map(_.child.map(_.text).toArray)
      .getOrElse(
        throw new NoSuchElementException(
          s"Property $DataStoreName was not set"))

  override def init(configuration: IConfiguration): Unit =
    time(s"Initializing SparkSQL connector") {}

  override def connect(
    credentials: ICredentials,
    config: ConnectorClusterConfig): Unit =
    time("Connecting to SparkSQL connector") {

      clusterConfig = Option(config)

      time("Creating SparkContext") {
        sparkContext.foreach(_.stop())
        sparkContext = Option(initContext(connectorConfig))
      }

      time(s"Creating $sqlContextType from SparkContext") {
        sqlContext = Option(sqlContextBuilder(sqlContextType, sparkContext.get))
      }

      time("Creating QueryManager") {
        queryManager = Option(
          system.actorOf(
            QueryManager(
              queryExecutors,
              sqlContext.get,
              (workflow, sqlContext) =>
                QueryEngine.executeQuery(
                  workflow,
                  sqlContext,
                  config,
                  provider))))
      }

      time("Subscribing to metadata updates...") {
        sqlContext.foreach { sqlCtx =>
          connectorApp.subscribeToMetadataUpdate(
            SparkSQLMetadataListener(
              sqlCtx,
              sparkSQLConnector.provider,
              config))
        }
      }

    }

  override def getQueryEngine: IQueryEngine =
    (for {
      sqlCtx <- sqlContext
      qm <- queryManager
      conf <- clusterConfig
    } yield new QueryEngine(sqlCtx, qm, conf, provider)).getOrElse {
      throw new IllegalStateException("SparkSQL connector is not connected")
    }

  override def isConnected(name: ClusterName): Boolean =
    clusterConfig.exists(_.getName == name)

  override def close(name: ClusterName): Unit =
    time("Closing connection to $name cluster") {
      clusterConfig = None
    }

  override def shutdown(): Unit =
    time("Shutting down connector...") {
      sqlContext = None
      logger.debug("Disposing QueryManager")
      queryManager.foreach(_ ! Kill)
      queryManager = None
      logger.debug("Disposing SparkContext")
      sparkContext.foreach(_.stop())
      logger.debug("Connector was shutted down.")
    }

  //  Helpers

  /**
   * Build a brand new Spark context given some config parameters.
   * @param config Configuration object.
   * @return A new Spark context
   */
  def initContext(config: Config): SparkContext = {
    import scala.collection.JavaConversions._
    new SparkContext(new SparkConf()
      .setAppName(SparkSQLConnector.SparkSQLConnectorJobConstant)
      .setSparkHome(config.getString(SparkHome))
      .setMaster(config.getString(SparkMaster))
      .setJars(config.getConfig(Spark).getStringList(SparkJars))
      .setAll(List(
      SparkDriverMemory,
      SparkExecutorMemory,
      SparkTaskCPUs).map(k => k -> config.getString(k))))
  }

  /**
   * Build a new SQLContext depending on given type.
   *
   * @param contextType 'HiveContext' and 'SQLContext' (by default) are,
   *                    by now, the only supported types.
   * @param sc SparkContext used to build new SQLContext.
   * @return A brand new SQLContext.
   */
  def sqlContextBuilder(
    contextType: String,
    sc: SparkContext): SparkSQLContext =
    contextType match {
      case HIVEContext => new HiveContext(sc) with WithCatalog
      case _ => new SQLContext(sc) with WithCatalog
    }

  //  Unsupported methods

  override def getSqlEngine: ISqlEngine =
    throw new UnsupportedException(SparkSQLConnector.MethodNotSupported)

  override def getStorageEngine: IStorageEngine =
    throw new UnsupportedException(SparkSQLConnector.MethodNotSupported)

  override def getMetadataEngine: IMetadataEngine =
    throw new UnsupportedException(SparkSQLConnector.MethodNotSupported)

}

object SparkSQLConnector extends App
with Constants
with Configuration
with Loggable {

  import timer._

  val system =
    time(s"Initializing '$ActorSystemName' actor system...") {
      ActorSystem(ActorSystemName)
    }

  val sparkSQLConnector =
    time(s"Building SparkSQLConnector...") {
      new SparkSQLConnector(system)
    }

  val connectorApp =
    time("Starting up connector...") {
      val ca = new ConnectorApp
      ca.startup(sparkSQLConnector)
      ca
    }

  system.registerOnTermination {
    time("Termination detected. Shutting down actor system...") {
      sparkSQLConnector.shutdown()
    }
  }

}

sealed trait Constants {

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
  val connectorConfig: Config = {
    val input = Option(getClass.getClassLoader.getResourceAsStream(
      SparkSQLConnector.ConfigurationFileConstant))
    input.fold {
      val message = s"Sorry, unable to find [${
        SparkSQLConnector.ConfigurationFileConstant
      }]"
      logger.error(message)
      throw new InitializationException(message)
    }(_ => ConfigFactory.load(SparkSQLConnector.ConfigurationFileConstant))
  }

  //  References to 'SparkSQLConnector'
  val connectorConfigFile =
    XML.load(getClass.getClassLoader.getResourceAsStream(ConnectorConfigFile))

}