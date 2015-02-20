package com.stratio.connector.sparksql.core.connection


import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IConfiguration}
import com.stratio.crossdata.common.exceptions.ConnectionException
import com.stratio.crossdata.common.security.ICredentials
import com.stratio.connector.commons.connection.{ConnectionHandler,Connection}

/**
 * Created by jmgomez on 20/02/15.
 */
class SparkSQLConnectionHandler(configuration: IConfiguration) extends  ConnectionHandler(configuration){




  @throws(classOf[ConnectionException])
  protected def createNativeConnection(credentials: ICredentials, connectorClusterConfig: ConnectorClusterConfig):
  Connection[SparkSQLConnection] = {

  }
}
