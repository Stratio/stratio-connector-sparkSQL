package com.stratio.connector.sparksql

/**
 * Created by jmgomez on 16/04/15.
 */

import java.net.URL
import java.util.Collections
import com.stratio.connector.commons._

import akka.actor.{ActorSystem, ActorRefFactory}
import com.stratio.connector.sparksql.SparkSQLConnector._
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.data.ClusterName
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow
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




def executeQuery( query : String,sparkSQLConnector: SparkSQLConnector): Unit ={

  println(s"New query ---------------------------------------->$query")
  val workFlow = new LogicalWorkflow(Collections.emptyList())
  workFlow.setSqlDirectQuery(query)
  print(sparkSQLConnector.getQueryEngine.execute(workFlow).getResultSet.size())
}





}
