package com.stratio.connector.sparksql.elasticsearch

import com.stratio.connector.sparksql.core.providerConfig.{Constants, Provider}

/**
 * Created by pmadrigal on 29/06/15.
 */
case object ElasticSearch  extends Provider with Constants {
  override val dataSource: String = "org.elasticsearch.spark.sql"
}
