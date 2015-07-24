package com.stratio.connector.sparksql.elasticsearch

import com.stratio.connector.sparksql.core.{Constants, Provider}

case object ElasticSearch extends Provider with Constants {

  override val dataSource: String = "org.elasticsearch.spark.sql"

  override val manifest: String = "ElasticsearchDataStore.xml"

  override val name: String = "elasticsearch"

}