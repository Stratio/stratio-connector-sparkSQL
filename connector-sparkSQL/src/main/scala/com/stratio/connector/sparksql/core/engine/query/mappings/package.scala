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
package com.stratio.connector.sparksql.core.engine.query.mappings

import com.stratio.crossdata.common.metadata.{DataType, ColumnType}


/**
 * Function type helpers for getting column metadata from LogicalWorkflow.
 */
object `package` {

  private val SQLDate = "SQL_DATE"

  val functionType = Map(
    "sum" -> new ColumnType(DataType.FLOAT),
    "avg"->new ColumnType(DataType.FLOAT),
    "max" -> new ColumnType(DataType.FLOAT),
    "min" -> new ColumnType(DataType.FLOAT),
    "count" -> new ColumnType(DataType.INT),
    "substr" -> new ColumnType(DataType.VARCHAR),
    "substring" -> new ColumnType(DataType.VARCHAR),
    "to_date" -> {
      val columnName = new ColumnType(DataType.NATIVE)
      columnName.setODBCType(SQLDate)
      columnName
    },
    "datediff" -> new ColumnType(DataType.INT),
    "year" ->  new ColumnType(DataType.VARCHAR),
    "month" ->  new ColumnType(DataType.VARCHAR),
    "day" ->  new ColumnType(DataType.VARCHAR),
    "date_add" ->  {
      val columnName = new ColumnType(DataType.NATIVE)
      columnName.setODBCType(SQLDate)
      columnName
    },
    "date_sub" -> {
      val columnName = new ColumnType(DataType.NATIVE)
      columnName.setODBCType(SQLDate)
      columnName
    }
  )

}
