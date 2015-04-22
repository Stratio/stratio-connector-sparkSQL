package com.stratio.connector.sparksql.engine.query.mappings

import com.stratio.crossdata.common.metadata.{DataType, ColumnType}

/**
 * Created by jmgomez on 22/04/15.
 */
object functionType {

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
      columnName.setODBCType("SQL_DATE")
      columnName
    },
    "datediff" -> new ColumnType(DataType.INT),
    "year" ->  new ColumnType(DataType.VARCHAR),
    "month" ->  new ColumnType(DataType.VARCHAR),
    "day" ->  new ColumnType(DataType.VARCHAR),
    "date_add" ->  {
      val columnName = new ColumnType(DataType.NATIVE)
      columnName.setODBCType("SQL_DATE")
      columnName
    },
    "date_sub" -> {
      val columnName = new ColumnType(DataType.NATIVE)
      columnName.setODBCType("SQL_DATE")
      columnName
    }
  )

}
