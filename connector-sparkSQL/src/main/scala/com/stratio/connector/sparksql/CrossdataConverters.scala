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
package com.stratio.connector.sparksql

import com.stratio.connector.commons.Loggable
import com.stratio.connector.sparksql.core.`package`.SparkSQLContext
import com.stratio.crossdata.common.data.{Cell, ResultSet, Row => XDRow}
import com.stratio.crossdata.common.metadata.{ColumnMetadata, ColumnType}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row => SparkSQLRow}

import com.stratio.crossdata.common.metadata.{DataType => XDdataType}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.Try

/**
 * Provides the way to create Crossdata structures given some Spark's.
 */
object CrossdataConverters extends Loggable {

  import scala.collection.JavaConversions._

  type ColumnTypeMap = Map[String, ColumnType]

  type SparkSQLValue = Any

  type XDCell = Any

  type SparkSQLType = StructType

  /**
   * Compute some SchemaRDD and map it into a Crossdata ResultSet
   * @param dataFrame Given Schema RDD
   * @param metadata Columns metadata
   * @return An equivalent ResultSet
   */
  def toResultSet(
                   dataFrame: DataFrame,
                   metadata: List[ColumnMetadata]): ResultSet =
    toResultSet(dataFrame.rdd.toLocalIterator, dataFrame.schema, metadata)

  /**
   * Compute some SchemaRDD and map it into a Crossdata ResultSet
   * @param rows Iterator of Crossdata rows to be converted
   * @param schema Row structure
   * @param metadata Columns metadata
   * @return A new Resultset with all converted rows
   */
  def toResultSet(
                   rows: Iterator[SparkSQLRow],
                   schema: StructType,
                   metadata: List[ColumnMetadata]): ResultSet = {
    val resultSet = new ResultSet
    logger.debug(s"Metadata : $metadata")
    resultSet.setColumnMetadata(metadata)
    logger.debug(s"Generating result set...")
    val groupSize = 5000
    rows.grouped(groupSize).zipWithIndex.foreach {
      case (it, index) =>
        it.foreach(row => resultSet.add(toCrossDataRow(row, schema, metadata)))
        logger.debug(s"The connector has inserted ${index * groupSize} rows in the resultset")
    }

    logger.info(s"Result set size : ${resultSet.size()}")
    resultSet
  }

  /**
   * Convert some SparkSQL row into a Crossdata row
   * @param row SparkSQL row to be converted
   * @param schema Row structure
   * @param metadata Crossdata column metadata
   * @return A new Resultset with all converted rows
   */
  def toCrossDataRow(
    row: SparkSQLRow,
    schema: StructType,
    metadata: List[ColumnMetadata]): XDRow = {
    val fields = schema.fields
    val xdRow = new XDRow()
    fields.zipWithIndex.zip(metadata).foreach {
      case ((field, idx),meta) => {
        xdRow.addCell(
          meta.getName.getColumnNameToShow,
          new Cell(toCellValue(row(idx), field.dataType)))
      }
    }
    xdRow
  }

  /**
   * Convert some undefined value into Crossdata cell
   * @param value Given value to be converted to cell
   * @param dataType Object data type.
   * @return A new Crossdata cell with converted object.
   */
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

  def toSchemaRDD(resultSet: ResultSet, sparkSQLContext: SparkSQLContext) : DataFrame = {
    logger.debug(s"ResultSet count: ${resultSet.size()}" )
    val metadata = resultSet.getColumnMetadata.toList
    val xdRows = resultSet.getRows
    val rows: Iterable[(SparkSQLRow,SparkSQLType)] = xdRows.map{
      case (row) => {
        toSparkSQLRow(deduceFieldsType(row, metadata))
      }
    }
    val rowRdd = sparkSQLContext.sparkContext.parallelize(rows.map(_._1).toSeq)
    val schema = rows.head._2
    logger.info(s"Schema deduced from resultset: $schema")
    val count = rowRdd.count()
    logger.info(s"Number of rows in dataframe: ${rowRdd.count}")
    val df = sparkSQLContext.createDataFrame(rowRdd, schema)
    df
  }

  def deduceFieldsType (row: XDRow, metadata: List[ColumnMetadata]) :
  Iterable [(Any, String, XDdataType)] = {
    val (succedeed,failed) = row.getCellList.zipWithIndex.map {
      case (cell, count) =>
        Try {
          (cell.getValue, metadata.get(count).getName.getName,
            metadata.get(count).getColumnType.getDataType)
        }
    }.partition(_.isSuccess)
    succedeed.map(_.get)
  }


  def toSparkSQLRow (it: Iterable [(Any, String, XDdataType)]) : (SparkSQLRow, SparkSQLType) = {
    import com.stratio.crossdata.common.metadata.DataType._
    val rowsMap = it.map {

      case (value, name, tpe) => value
    }

    val schemaMap = it.map {
      case (value: Int, name, INT) =>
        StructField(name, IntegerType, nullable=true)
      case (value: Double, name, DOUBLE) =>
        StructField(name, DoubleType, nullable=true)
      case (value: String, name, TEXT) =>
        StructField(name, StringType, nullable=true)
      case (value: String, name, VARCHAR) =>
        StructField(name, StringType, nullable=true)
      case (value: Boolean, name, BOOLEAN) =>
        StructField(name, BooleanType, nullable=true)
      case (value, name, BIGINT) =>
        StructField(name, IntegerType, nullable=true)

    }
    val row = SparkSQLRow.fromSeq(rowsMap.toSeq)
    val sparkSQLType = StructType(schemaMap.toList)
    (row, sparkSQLType)
  }







}
