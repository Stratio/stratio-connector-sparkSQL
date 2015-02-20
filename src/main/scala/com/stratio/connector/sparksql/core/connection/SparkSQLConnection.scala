package com.stratio.connector.sparksql.core.connection

import com.stratio.connector.commons.connection.Connection
import com.stratio.crossdata.common.connector.IConnector

/**
 * Created by jmgomez on 20/02/15.
 */
class SparkSQLConnection extends Connection[_]  {


  override def close(): Unit = ???

  override def getNativeConnection: _$1 = ???

  override def isConnected: Boolean = ???
}
