package com.stratio.connector.sparksql.connection

import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials

case class Connection(
  config: ConnectorClusterConfig,
  credentials: Option[ICredentials] = None,
  busy: Boolean = false,
  lastUseDate: Long = System.currentTimeMillis())
