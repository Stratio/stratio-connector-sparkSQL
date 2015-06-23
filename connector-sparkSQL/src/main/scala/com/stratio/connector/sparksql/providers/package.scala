/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.sparksql.providers

/**
 * Router for every single supported provider.
 */
object `package` {

  val ParquetProvider = "hdfs"
  val CassandraProvider = "Cassandra"
  val HBaseProvider = "hbase"
  val MongoProvider = "Mongo"

  val manifests = Map(
    ParquetProvider -> "HDFS",
    CassandraProvider -> "Cassandra",
    HBaseProvider -> "HBase",
    MongoProvider -> "Mongo"
  ).mapValues(name => s"${name}DataStore.xml")

  val all = List(
    ParquetProvider,
    CassandraProvider,
    HBaseProvider,
    MongoProvider
  )

  def apply(providerName: String): Option[Provider] = providerName match {
    case ParquetProvider => Some(Parquet)
    case CassandraProvider => Some(Cassandra)
    case HBaseProvider => Some(HBase)
    case MongoProvider => Some (MongoDB)
    case _ => None
  }

}
