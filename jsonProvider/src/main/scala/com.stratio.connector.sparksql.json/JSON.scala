package com.stratio.connector.sparksql.json

import com.stratio.connector.sparksql.core.Provider

case object JSON extends Provider {

  override val manifest: String = "JSONDataStore.xml"

  override val name: String = "json"

  val dataSource = "org.apache.spark.sql.json"

}