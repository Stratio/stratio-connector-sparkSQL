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

  /**
   * How to create a connection from a cluster that uses
   * this provider
   *
   * @param config Cluster configuration options
   * @param sqlContext Spark SQLContext
   * @param credentials Used credentials
   * @return A brand new cluster connection.
   */
  def createConnection(
    config: ConnectorClusterConfig,
    sqlContext: SQLContext,
    credentials: Option[ICredentials] = None): Connection =
    new Connection(config,credentials)

  /**
   * It formats given SQL statement, adapting it to
   * current involved provider SQL language.
   * @param statement Given SQL statement
   * @param options Option map
   * @return The formatted statement
   */
  def formatSQL(
    statement: String,
    options: Map[String,String] = Map()): String = statement

}