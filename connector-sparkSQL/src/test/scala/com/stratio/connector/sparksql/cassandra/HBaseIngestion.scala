package com.stratio.connector.sparksql.cassandra

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.scalacheck.Gen

object HBaseIngestion extends App with Generators{

  lazy val sparkConf = new SparkConf()
    .setAppName("HBaseIngestion")
    .setMaster("spark://benchmark1.stratio.com:7077")
    .setAll(List("spark.hadoop.hbase.zookeeper.quorum" -> "benchmark1"))

  lazy val sparkContext = new SparkContext(sparkConf)

  lazy val hbaseContext: HBaseSQLContext = new HBaseSQLContext(sparkContext)

  val tableName = s"clientes"

  val sparkTableName = s"${tableName}_${System.currentTimeMillis()}"

  val sampleSize = 10

  hbaseContext.sql(s"""CREATE TABLE $sparkTableName (id STRING, name STRING, surname STRING,
                     age INTEGER, PRIMARY KEY (id))
                    MAPPED BY ($tableName, COLS=[name=`data.name`, surname=`data.surname`, age=`data.age`])""")

  Gen.listOfN(sampleSize,customerGen(sparkTableName)).sample.getOrElse(List())
    .foreach { statement =>
    hbaseContext.sql(statement)
  }

  sparkContext.stop()

}

trait Generators {

    def customerGen(tableName: String): Gen[String] = for {
      name <- nameGen
      surname <- surnameGen
      age <- ageGen
    } yield s"""INSERT INTO $tableName VALUES ("${java.util.UUID.randomUUID.toString}","$name","$surname",$age)"""

    //  Auxiliar generators

    val nameGen = Gen.oneOf(
      "George",
      "John",
      "Mary",
      "Alfred",
      "Bruce",
      "Johana")

    val surnameGen = Gen.oneOf(
      "Hopkins",
      "Wayne",
      "Hitchcock",
      "Fernandez",
      "Seagull",
      "VanDamme")

    val ageGen = Gen.choose(16,40)

}