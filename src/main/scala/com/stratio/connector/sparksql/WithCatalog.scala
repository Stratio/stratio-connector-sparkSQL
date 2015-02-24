package com.stratio.connector.sparksql

import org.apache.spark.sql.SQLContext

/**
 * Provides an accessor to SQLContext catalog.
 */
trait WithCatalog { _: SQLContext =>

  def getCatalog = catalog

}