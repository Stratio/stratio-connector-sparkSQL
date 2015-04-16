package com.stratio.connector.sparksql.providers

/**
 * Represents a custom SparkSQL Data Source.
 */
trait Provider {

  /**
   * DefaultSource qualifed package name
   */
  val datasource: String

}