package com.stratio.connector.sparksql.parquet

import com.stratio.connector.sparksql.core.Provider

case object Parquet extends Provider {

  override val manifest: String = "HDFSDatastore.xml"

  override val name: String = "hdfs"

  val dataSource = "org.apache.spark.sql.parquet"

}