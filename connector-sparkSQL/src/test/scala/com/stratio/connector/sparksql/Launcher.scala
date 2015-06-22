package com.stratio.connector.sparksql

/**
 * Created by jmgomez on 16/04/15.
 */

import java.net.URL
import java.util.Collections
import com.stratio.connector.commons._
import scala.collection.JavaConversions._
import akka.actor.{ActorSystem, ActorRefFactory}
import com.stratio.connector.sparksql.SparkSQLConnector._
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.data.{TableName, ClusterName}
import com.stratio.crossdata.common.logicalplan.{Project, LogicalWorkflow}
import com.stratio.crossdata.common.metadata.Operations
import com.stratio.crossdata.connectors.ConnectorApp

import scala.io.Source

object Launcher extends App with Constants
with Configuration
with Loggable
with Metrics {


  val actorRefFactory =  ActorSystem("Test")
  var sparkSQLConnector = new SparkSQLConnector(actorRefFactory) // { override val provider=Parquet}

  sparkSQLConnector.init(null)


  sparkSQLConnector.connect(null, new ConnectorClusterConfig(new ClusterName("hdfs"),Collections.emptyMap(),Collections.emptyMap()))




   Source.fromURL( getClass.getResource("/queries.txt")).getLines().foreach(executeQuery(_,sparkSQLConnector))







def executeQuery( command : String,sparkSQLConnector: SparkSQLConnector): Unit ={

  if (!command.contains("#") && command.size !=0) {
    (1 to 4).foreach(_ => println((1 to 200).map(_ => "*").mkString("")))
    println(s"New query ---------------------------------------->$command")
    (1 to 4).foreach(_ => println((1 to 200).map(_ => "*").mkString("")))


    val (catalog::sql::Nil)  = command.split("\\|").toList



    val workFlow = new LogicalWorkflow(List(
      new Project(
        Set[Operations](),
        new TableName(catalog.trim,"table"),
        new ClusterName("cluster1"))
    )
    )
    
    

    
    workFlow.setSqlDirectQuery(sql)
    val mod = 10000000;
    (1 to 1).toList.map(_.toString).map(x => new Thread(
      new Runnable {

        var queryId: String = ???

        def run() {
          val timeInit = System.currentTimeMillis() % mod
          println(s"Query $x: Start: $timeInit");
          println(s"------------------->>> Filas devueltas ${sparkSQLConnector.getQueryEngine.execute(queryId, workFlow).getResultSet.size()}")
          (1 to 2).foreach(_ => println((1 to 200).map(_ => "*").mkString("")))
          val timeFinish = System.currentTimeMillis() % mod
          println(s"Query $x: Finish: $timeFinish")
          println(s"Time Query $x, ${timeFinish - timeInit}")
        }
      })).foreach(_.start)
  }






}





}
