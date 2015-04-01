package com.stratio.connector.sparksql.engine.query

import akka.actor.{Actor, ActorRef, Props}
import com.stratio.connector.sparksql.connection.ConnectionHandler
import com.stratio.connector.sparksql.engine.query.QueryExecutor.DataFrameProvider
import com.stratio.connector.sparksql.engine.query.QueryManager.{Finished, AsyncExecute}
import com.stratio.connector.sparksql.{Parquet, Catalog, UnitTest}
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.exceptions.ExecutionException
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow
import com.stratio.crossdata.common.result.QueryResult
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import scala.collection.JavaConversions._
import com.stratio.crossdata.common.data.{Row => XDRow}

class QueryExecutorTest extends UnitTest("QueryExecutor") with Serializable {test =>

  //  Prepare workspace properties

  System.setProperty("hive.metastore.warehouse.dir","/tmp/warehouse")

  //  Initialize vars...

  val sc = new SparkContext(new SparkConf()
    .setMaster("local[4]")
    .setAppName("QueryExecutorTest"))
  val sqlContext = new HiveContext(sc) with Catalog
  val connectionHandler = new ConnectionHandler
  val provider: DataFrameProvider = (workflow, connectionHandler, sqlContext) =>
  QueryEngine.executeQuery(
        workflow,
        sqlContext,
        connectionHandler,
        Parquet)

  //  Populate table ...


  def generate(n: Int)(sqlContext: SQLContext): DataFrame = {
    val rdd = sc.parallelize(for {
        id <- 1 to n
    } yield Student(id, s"Name$id"), 4)
    sqlContext.createDataFrame(rdd)
  }

  val amount = 150

  generate(amount)(sqlContext).saveAsTable(
  "students",
  "org.apache.spark.sql.parquet",
      SaveMode.Overwrite,
      Map("path" -> "/tmp/students"))


  //  Test definition ...

  val (queryId,query) =
    "query-1" -> "SELECT catalog.students.id, catalog.students.name FROM catalog.students"


  it should "execute an async stopable job with same-sized chunks" in {

    val defaultChunkSize = amount / 3

    val lw = new LogicalWorkflow(List())
    lw.setSqlDirectQuery(query)
    val executor = system.actorOf(Props(new Actor {
      val slave = context.actorOf(Props(new QueryExecutor(
        sqlContext,
        defaultChunkSize,
        provider,
        connectionHandler,
        asyncStoppable = true)))
      slave ! AsyncExecute(
        queryId,
        lw,
        resultHandler(self))
      def receive = {
        case msg => test.self forward msg
      }
    }))
    receiveN(amount).forall(_.isInstanceOf[XDRow])
    expectMsg(Finished(queryId))

  }

  it should "execute an async non-stopable job with same result as an stoppable one" in {

    val defaultChunkSize = amount / 3

    val lw = new LogicalWorkflow(List())
    lw.setSqlDirectQuery(query)
    val executor = system.actorOf(Props(new Actor {
      val slave = context.actorOf(Props(new QueryExecutor(
        sqlContext,
        defaultChunkSize,
        provider,
        connectionHandler,
        asyncStoppable = false)))
      slave ! AsyncExecute(
        queryId,
        lw,
        resultHandler(self))
      def receive = {
        case msg => test.self forward msg
      }
    }))
    receiveN(amount).forall(_.isInstanceOf[XDRow])
    expectMsg(Finished(queryId))

  }

  //  Helpers

  def resultHandler(responseTo: ActorRef) = new IResultHandler {
    override def processException(
      queryId: String,
      exception: ExecutionException): Unit = fail(exception)

    override def processResult(result: QueryResult): Unit = {
      result.getResultSet.iterator.foreach(responseTo ! _)
    }
  }

}

case class Student(id: Int, name: String)
