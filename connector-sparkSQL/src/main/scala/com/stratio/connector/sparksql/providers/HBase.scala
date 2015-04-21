package com.stratio.connector.sparksql.providers

import com.stratio.connector.sparksql.connection.{Connection => SparkSQLConnection}
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hbase.HBaseSQLContext

case object HBase extends Provider {

  override val datasource: String = "org.apache.spark.sql.hbase.HBaseSource"

}
