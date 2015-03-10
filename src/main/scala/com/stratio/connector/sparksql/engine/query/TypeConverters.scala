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

package com.stratio.connector.sparksql.engine.query

import com.stratio.connector.sparksql.Loggable
import com.stratio.connector.commons.timer
import com.stratio.crossdata.common.metadata.{ColumnMetadata, ColumnType}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.types.{ArrayType, DataType, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import com.stratio.crossdata.common.data.{Row => XDRow, Cell, ResultSet}
import org.apache.spark.sql.{Row, SchemaRDD}

object TypeConverters extends Loggable {

  import timer._
  import scala.collection.JavaConversions._

  type ColumnTypeMap = Map[String, ColumnType]

  def toResultSet(
    schemaRDD: SchemaRDD,
    metadata:List[ColumnMetadata]): ResultSet =
    toResultSet(schemaRDD.toLocalIterator, schemaRDD.schema,metadata)

  def toResultSet(
    rows: Iterator[Row],
    schema: StructType,
    metadata:List[ColumnMetadata]): ResultSet = {
    time(s"Mapping Spark SQL rows into result set.\nSchema: ${schema.treeString}") {
      val resultSet = new ResultSet
      resultSet.setColumnMetadata(metadata)
      rows.foreach(row => resultSet.add(toCrossDataRow(row, schema)))
      resultSet
    }
  }

  def toCrossDataRow(row: Row, schema: StructType): XDRow = {
    val fields = schema.fields
    val xdRow = new XDRow()
    fields.zipWithIndex.map {
      case (field, idx) => xdRow.addCell(
        field.name,
        new Cell(toCellValue(row(idx), field.dataType)))
    }
    xdRow
  }

  def toCellValue(value: Any, dataType: DataType): Any = {
    import scala.collection.JavaConversions._
    Option(value).map { v =>
      (dataType, value) match {
        case (ArrayType(elementType, _), array: ArrayBuffer[Any@unchecked]) =>
          val list: java.util.List[Any] = array.map {
            case obj => toCellValue(obj, elementType)
          }.toList
          list
        case (struct: StructType, value: GenericRow) =>
          val map: java.util.Map[String, Any] = struct.fields.zipWithIndex.map {
            case (field, idx) =>
              field.name -> toCellValue(value(idx), field.dataType)
          }.toMap[String, Any]
          map
        case _ =>
          value
      }
    }.orNull
  }

}
