package com.stratio.connector.sparksql.cassandra

private[sparksql] trait CassandraConstants {
  val CassandraConnectionHostProperty = "spark.cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "spark.cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "spark.cassandra.connection.native.port"

}
