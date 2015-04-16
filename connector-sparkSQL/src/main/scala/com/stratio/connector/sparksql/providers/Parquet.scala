package com.stratio.connector.sparksql.providers

case object Parquet extends Provider {

  val datasource = "org.apache.spark.sql.parquet"

}