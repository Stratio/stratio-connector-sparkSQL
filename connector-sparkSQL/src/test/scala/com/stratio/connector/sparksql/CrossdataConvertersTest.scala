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

import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._

class CrossdataConvertersTest extends Test("CrossdataConverters") {

  it should "convert any value in a SparkSQLRow into a Crossdata cell value" in {

    type InputValue = (Any, DataType)
    type OutputValue = Any
    type ExpectedValues = Map[InputValue, OutputValue]

    val expected = Map(
      ("hi", StringType) -> "hi",
      (Int.MaxValue, IntegerType) -> Int.MaxValue,
      (true, BooleanType) -> true,
      ({
        val array = new ArrayBuffer[Int]
        array += Int.MaxValue
        array
      }, ArrayType(IntegerType,containsNull = false)) -> {
        val list: java.util.List[Int] = List(Int.MaxValue)
        list
      },
      ({
        val array = new ArrayBuffer[ArrayBuffer[Int]]
        array += {
          val array = new ArrayBuffer[Int]
          array += Int.MaxValue
          array
          }
        array
      }, ArrayType(ArrayType(IntegerType,containsNull = false),containsNull = false)) -> {
        val innerList: java.util.List[Int] = List(Int.MaxValue)
        val list: java.util.List[java.util.List[Int]] = List(innerList)
        list
      },
      ( {
        new GenericRow(Array[Any](1, true, "hi", {
          val array = new ArrayBuffer[Int]
          array += Int.MaxValue
          array
        }))
      }, new StructType(Array(
        new StructField("field1", IntegerType),
        new StructField("field2", BooleanType),
        new StructField("field3", StringType),
        new StructField("field4", ArrayType(IntegerType))))) -> {
        val map: java.util.Map[String, Any] = Map[String, Any](
          "field1" -> 1,
          "field2" -> true,
          "field3" -> "hi",
          "field4" -> {
            val list: java.util.List[Int] = List(Int.MaxValue)
            list
          })
        map
      })

    expected.foreach {
      case ((input, tpe), output) =>
        CrossdataConverters.toCellValue(input, tpe) should equal(output)
    }

  }

}

