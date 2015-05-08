package com.stratio.connector.sparksql.providers

case object Parquet extends Provider {

  val dataSource = "org.apache.spark.sql.parquet"

}