package com.stratio.connector.sparksql.providers

import com.stratio.connector.sparksql.Catalog
import org.apache.spark.SparkContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.types.{StructField, StructType}

case object HBase extends CustomContextProvider[HBaseSQLContext with Catalog] {

  val datasource: String = "org.apache.spark.sql.hbase.HBaseSource"

  val catalogPersistence = false

  def buildContext(sc: SparkContext) = new HBaseSQLContext(sc) with Catalog

}
