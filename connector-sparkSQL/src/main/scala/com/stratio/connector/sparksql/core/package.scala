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
package com.stratio.connector.sparksql.core

import com.stratio.connector.commons.Loggable
import com.stratio.crossdata.common.exceptions.InitializationException
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SQLContext

import scala.util.Try
import scala.xml.{Elem, XML}

object `package`{

  type SparkSQLContext = SQLContext with Catalog

}

object providers {

  val providers: Array[String] = SparkSQLConnector.providersFromConfig

  def providerByName(name: String): Provider = {
    val clazz = Class.forName(name + "$")
    clazz.getField("MODULE$").get(clazz).asInstanceOf[Provider]
  }

  lazy val all: Array[Provider] = providers.map(providerByName)

  /*
    def all: Array[Provider] = providers.map(providerByName)
  */

  def manifests = all.map(_.manifest)

  def apply(name: String): Option[Provider] = {
    all.map(p => p.name -> p).toMap.get(name)
  }

  /*  val ParquetProvider = "hdfs"

    val manifests = Map(
      ParquetProvider -> "HDFS").mapValues(name => s"${name}DataStore.xml")*/

  // def apply(providerName: String): Option[Provider] = {

  //    providerName match {
  //    case ParquetProvider => Some(Parquet)
  //    case CassandraProvider => Some(Cassandra)
  //    case HBaseProvider => Some(HBase)
  //    case MongoProvider => Some (MongoDB)
  //    case _ => None
  // }


  //******************************************************


  /*Array(
    "MongoDB",
    "HBase",
    "Cassandra")*/

  // val classes = getClass.getClassLoader
  /*lazy val providersInUse = new{

    val all: Array[Provider] = classes.map(provider => Class.forName(provider).newInstance().asInstanceOf[Provider])
  }*/

}



/**
 * Provides an accessor to SQLContext catalog.
 */
trait Catalog {
  _: SQLContext =>

  def getCatalog = catalog

}

/**
 * Configuration stuff related to SparkSQLConnector.
 */
trait Configuration extends Constants{
  _: Loggable =>

  //  References to 'connector-application'
  val connectorConfig: Try[Config] = {
    val input = Option(getClass.getClassLoader.getResourceAsStream(
      ConfigurationFileConstant))
    Try(input.fold {
      val message = s"Sorry, unable to find [${
        ConfigurationFileConstant
      }]"
      logger.error(message)
      throw new InitializationException(message)
    }(_ => ConfigFactory.load(ConfigurationFileConstant)))
  }

  //  References to 'SparkSQLConnector'
  val connectorConfigFile: Try[Elem] =
    Try(XML.load(
      getClass.getClassLoader.getResourceAsStream(ConnectorConfigFile)))

}

trait Constants {

  //  Constants

  val ActorSystemName = "SparkSQLConnectorSystem"
  val ConfigurationFileConstant = "connector-application.conf"
  val SparkMaster = "spark.master"
  val SparkHome = "spark.home"
  val SparkDriverMemory = "spark.driver.memory"
  val SparkExecutorMemory = "spark.executor.memory"
  val SparkTaskCPUs = "spark.task.cpus"
  val SparkJars = "jars"
  val MethodNotSupported = "Not supported yet"
  val SparkSQLConnectorJobConstant = "SparkSQLConnectorJob"
  val Spark = "spark"
  val SparkCoresMax = "spark.cores.max"
  val ConnectorConfigFile = "SparkSQLConnector.xml"
  val ConnectorName = "ConnectorName"
  val DataStoreName = "DataStoreName"
  val SQLContext = "SQLContext"
  val HIVEContext = "HiveContext"
  val CountApproxTimeout = "connector.count-approx-timeout"
  val QueryExecutorsAmount = "connector.query-executors.size"
  val SQLContextType = "connector.sql-context-type"
  val ProvidersInUse = "datastore.providers"
  val AsyncStoppable = "connector.async-stoppable"
  val ChunkSize = "connector.query-executors.chunk-size"
  val CatalogTableSeparator = "_"

}