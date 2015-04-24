package com.stratio.connector.sparksql.providers

import com.stratio.connector.sparksql.Test

class HBaseTest extends Test("HBase"){

  it should "replace all crossdata mapped fields in given statement" in {

    val query = "SELECT table1.field1, table1.field2, table1.field3 FROM table1"

    val options = Map(
      HBase.MappedFieldsOption -> "field1->cf1:cq1,field2->cf1:cq2,field3->cf2:cq1")

    HBase.formatSQL(query,options) should equal(
      "SELECT table1.`cf1:cq1`, table1.`cf1:cq2`, table1.`cf2:cq1` FROM table1")

  }

}
