package com.stratio.connector.sparksql.providers

/**
 * Router for every single supported provider.
 */
object `package`{

  val ParquetProvider = "hdfs"

  def apply(providerName: String): Option[Provider] = providerName match {
    case ParquetProvider => Some(Parquet)
    case _ => None
  }

}
