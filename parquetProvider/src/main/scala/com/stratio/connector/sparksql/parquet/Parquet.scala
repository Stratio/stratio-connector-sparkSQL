package com.stratio.connector.sparksql.parquet



case object Parquet extends Provider {

  override val manifest: String = "HDFSDatastore.xml"

  override val name: String = "hdfs"

  val dataSource = "org.apache.spark.sql.parquet"

}