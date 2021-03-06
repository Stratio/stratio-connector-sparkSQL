package com.stratio.connector.sparksql.core

import com.stratio.connector.commons.Loggable
import com.stratio.connector.sparksql.core.connection.Connection
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Represents a custom SparkSQL Data Source.
 */
trait Provider extends Loggable {

  /**
   * DefaultSource qualified package name
   */
  val dataSource: String

  val manifest: String

  val name: String

  /**
   * Initialize current provider
   * @param sc Driver SparkContext
   */
  def initialize(sc: SparkContext): Unit =
    logger.debug(s"Initializing ${getClass.getSimpleName} provider")

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
    credentials: Option[ICredentials] = None): Connection = {
    new Connection(config, credentials)
    }

  /**
   * It formats given SQL statement, adapting it to
   * current involved provider SQL language.
   * @param statement Given SQL statement
   * @param options Option map
   * @return The formatted statement
   */
  def formatSQL(
    statement: String,
    options: Map[String,Any] = Map()): String = statement


}