package com.stratio.connector.sparksql.providers

import com.stratio.connector.sparksql.connection.Connection
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.apache.spark.sql.SQLContext

/**
 * Represents a custom SparkSQL Data Source.
 */
trait Provider {

  /**
   * DefaultSource qualifed package name
   */
  val datasource: String

  def createConnection(
    config: ConnectorClusterConfig,
    sqlContext: SQLContext,
    credentials: Option[ICredentials] = None): Connection =
    new Connection(config,credentials)

}