package com.stratio.connector

import org.apache.spark.sql.SQLContext

package object sparksql {

  type Provider = String

  type SparkSQLContext = SQLContext with WithCatalog

}
