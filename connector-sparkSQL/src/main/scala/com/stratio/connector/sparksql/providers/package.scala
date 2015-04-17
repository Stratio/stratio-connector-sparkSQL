package com.stratio.connector.sparksql.providers

/**
 * Router for every single supported provider.
 */
object `package`{

  val ParquetProvider = "hdfs"
  val CassandraProvider = "cassandra"

  def apply(providerName: String): Option[Provider] = providerName match {
    case ParquetProvider => Some(Parquet)
    case CassandraProvider => Some(Cassandra)
    case _ => None
  }

}
