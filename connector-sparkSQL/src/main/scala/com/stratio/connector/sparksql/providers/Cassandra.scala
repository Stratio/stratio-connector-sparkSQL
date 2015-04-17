package com.stratio.connector.sparksql.providers

case object Cassandra extends Provider {
  override val datasource: String = "org.apache.spark.sql.cassandra"
}
