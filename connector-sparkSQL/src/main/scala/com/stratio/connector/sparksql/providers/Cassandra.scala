package com.stratio.connector.sparksql.providers

import com.stratio.connector.sparksql.Constants
import com.stratio.connector.sparksql.connection.Connection
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf

case object Cassandra extends Provider with Constants{

  override val datasource: String = "org.apache.spark.sql.cassandra"

  override def createConnection(
    config: ConnectorClusterConfig,
    sqlContext: SQLContext,
    credentials: Option[ICredentials]): Connection = {

    val conf= sqlContext.sparkContext.getConf
    val clusterOptions = config.getClusterOptions.toMap
    val clusterName = config.getName.getName
    val cassConfig = Map(
      CassandraConnectionHostProperty -> clusterOptions("hosts"),
      CassandraConnectionNativePortProperty ->
        clusterOptions.getOrElse("nativePort", "9042"),
      CassandraConnectionRpcPortProperty ->
        clusterOptions.getOrElse("rpcPort", "9160"))
    sqlContext.addClusterLevelCassandraConnConf(
      config.getName.getName,
      CassandraConnectorConf(conf.setAll(cassConfig)))
    super.createConnection(config,sqlContext,credentials)

  }

}
