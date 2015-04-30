package com.stratio.connector.sparksql.providers

import com.stratio.connector.sparksql.Catalog
import org.apache.spark.SparkContext
import org.apache.spark.sql.hbase.HBaseSQLContext

case object HBase extends CustomContextProvider[HBaseSQLContext with Catalog] {

  val datasource: String = "org.apache.spark.sql.hbase.HBaseSource"

  val catalogPersistence = false

  def buildContext(sc: SparkContext) = new HBaseSQLContext(sc) with Catalog

  val MappedFieldsOption = "mapped_fields"

  /**
   * Expected mapping fields in HBase will be as follows:{{{
   *   val options =
   *     Map("mapped_fields" -> "field1->cf1:cq1,field2->cf1:cq2,field3->cf2:cq1")
   * }}}
   * @param statement Given SQL statement
   * @param options Option map
   * @return The formatted statement
   */
  override def formatSQL(
    statement: String,
    options: Map[String, String] = Map()): String =
    (statement /: options.get(MappedFieldsOption)) {
      case (query, mapped) =>
        val mapping = parseMapping(mapped)
        (query /: mapping) {
          case (q, (crossdataField, hbaseField)) =>
            q.replaceAll(crossdataField, s"""`$hbaseField`""")
        }
    }

  /**
   * It parses mapped_fields format (i.e.: {{{
   * "field1->cf1:cq1,field2->cf1:cq2,field3->cf2:cq1"
   * }}}
   * @param mapped input mapped string
   * @return A map with split fields mapping.
   */
  private def parseMapping(mapped: String): Map[String, String] = mapped.split(",").map {
    case field =>
      val (crossdataField :: hbaseField :: Nil) = field.split("->").toList
      crossdataField -> hbaseField
  }.toMap

}
