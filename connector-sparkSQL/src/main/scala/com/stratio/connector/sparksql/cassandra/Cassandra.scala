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
package com.stratio.connector.sparksql.cassandra

import com.stratio.connector.sparksql.core.providerConfig.{Constants, Provider}
import com.stratio.connector.sparksql.core.connection.Connection
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf

case object Cassandra extends Provider with CassandraConstants{

  override val dataSource: String = "org.apache.spark.sql.cassandra"

  val DefaultNativePort = "9042"

  val DefaultRPCPort = "9160"

  override def createConnection(
    config: ConnectorClusterConfig,
    sqlContext: SQLContext,
    credentials: Option[ICredentials]): Connection = {

    val conf= sqlContext.sparkContext.getConf
    val clusterOptions = config.getClusterOptions.toMap
    val clusterName = clusterOptions("cluster")
    val keyspace = "benchmark"
    val cassConfig = Map(
      CassandraConnectionHostProperty -> clusterOptions("Hosts").replaceAll("\\[","").replaceAll("\\]",""),
      CassandraConnectionNativePortProperty ->
        clusterOptions.getOrElse("Port", DefaultNativePort),
      CassandraConnectionRpcPortProperty ->
        clusterOptions.getOrElse("rpcPort", DefaultRPCPort))
    cassConfig.map{
      prop => sqlContext.setConf(prop._1, prop._2)
    }
    sqlContext.setConf(s"$clusterName:$keyspace/spark.cassandra.connection.host", clusterOptions("Hosts").replaceAll("\\[","").replaceAll("\\]",""))
    logger.debug(s"The cache for [$clusterName] has been registering.")
    super.createConnection(config,sqlContext,credentials)
  }

}
