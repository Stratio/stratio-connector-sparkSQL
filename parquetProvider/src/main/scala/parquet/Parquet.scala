package parquet

import com.stratio.connector.sparksql.core.providerConfig.Provider

case object Parquet extends Provider {

  override val manifest: String = "HDFSDatastore.xml"

  override val name: String = "hdfs"

  val dataSource = "org.apache.spark.sql.parquet"

}