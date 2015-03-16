/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.connector.sparksql

import com.stratio.connector.sparksql
import org.apache.spark.sql.SchemaRDD

/**
 * Router for every single supported provider.
 */
object providers {

  val Parquet = "parquet"

  def apply(providerName: String): Provider = providerName match {
    case _ => sparksql.Parquet
  }

}

/**
 * Represents a custom SparkSQL Data Source.
 */
trait Provider {

  /**
   * DefaultSource qualifed package name
   */
  val datasource: String

  /**
   * Formats an Spark SQL obtained SchemaRDD,
   * adapting it to current provider format.
   * @param rdd SchemaRDD to be formatted
   * @return
   */
  def formatRDD(
    rdd: SchemaRDD,
    sqlContext: SparkSQLContext): SchemaRDD = rdd

}

case object Parquet extends Provider with Loggable {

  val datasource = "org.apache.spark.sql.parquet"

}