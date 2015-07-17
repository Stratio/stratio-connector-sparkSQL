package com.stratio.connector.sparksql.parquet

import com.stratio.connector.sparksql.core.providerConfig.Provider

case object Parquet extends Provider {

  val dataSource = "org.apache.spark.sql.parquet"

}