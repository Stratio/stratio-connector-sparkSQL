package com.stratio.connector.sparksql.providers

import com.stratio.connector.sparksql.`package`.SparkSQLContext
import org.apache.spark.SparkContext

trait CustomContextProvider[Context <: SparkSQLContext] extends Provider {

  var sqlContext: Option[Context] = None

  val catalogPersistence: Boolean

  def buildContext(sc: SparkContext): Context

  def initializeContext(sc: SparkContext): Unit =
    synchronized(sqlContext = Some(buildContext(sc)))

  override def initialize(sc: SparkContext): Unit = {
    super.initialize(sc)
    initializeContext(sc)
  }

}
