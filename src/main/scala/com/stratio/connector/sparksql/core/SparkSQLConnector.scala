package com.stratio.connector.sparksql.core

import com.stratio.connector.commons.CommonsConnector
import com.stratio.connector.commons.util.ManifestUtil
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler
import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.connectors.ConnectorApp
import com.stratio.crossdata.common.exceptions.{InitializationException,ExecutionException}
import org.slf4j.{LoggerFactory, Logger}

/**
 *
 * The SparkSQL connector.
 * Created by jmgomez on 20/02/15.
 */
class SparkSQLConnector extends CommonsConnector {

  /**
   * The connector name.
   */
  val connectorName: String = ManifestUtil.getConectorName("SparkSQLConnector.xml")

  /**
   * The datastore name.
   */
  val datastoreName: Array[String] = ManifestUtil.getDatastoreName("SparkSQLConnector.xml")


  /**
   * The Log.
   */
  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * return the connector name.
   * @return the connector name.
   */
  override def getConnectorName(): String = connectorName

  /**
   * Return the data stores names.
   * @return the data stores names.
   */
  override def getDatastoreName(): Array[String] = datastoreName

  override def init(configuration: IConfiguration) {
    connectionHandler = new SparkSQLConnectionHandler(configuration)
  }

  override def getMetadataEngine(): IMetadataEngine = ???

  override def getQueryEngine(): IQueryEngine = ???

  override def getStorageEngine(): IStorageEngine = ???


  /**
   * Attach shut down hook.
   */
  def attachShutDownHook() {
    Runtime.getRuntime.addShutdownHook( new Thread {
      override def run
      {
        try {
          shutdown
        }
        catch {
          case e: ExecutionException => {
            logger.error("Shutdown connector "+getConnectorName()+": "+ e.getMessage, e)
          }
        }
      }
    })
  }


  /**
   * The main method.
   *
   * @param args
     * the arguments
   * @throws InitializationException
     * if any error exists during the initialization
   */
  @throws(classOf[InitializationException])
  def main(args: Array[String]) {
    val sparkSQLConnector: SparkSQLConnector = new SparkSQLConnector()
    val connectorApp: ConnectorApp = new ConnectorApp
    connectorApp.startup(sparkSQLConnector)
    sparkSQLConnector.attachShutDownHook
  }


}
