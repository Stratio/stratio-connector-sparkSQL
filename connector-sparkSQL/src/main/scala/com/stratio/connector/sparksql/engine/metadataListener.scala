/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.sparksql.engine

import com.stratio.connector.commons.timer
import com.stratio.connector.sparksql.connection.ConnectionHandler
import com.stratio.connector.sparksql.{Metrics, Loggable, Provider, SparkSQLContext}
import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IMetadataListener}
import com.stratio.crossdata.common.metadata.{TableMetadata, IMetadata}
import org.slf4j.Logger
import com.stratio.connector.sparksql.engine.query.QueryEngine._

/**
 * Hook for receiving metadata update events.
 */
object SparkSQLMetadataListener extends Loggable with Metrics {

  import timer._

  def apply(
    sqlContext: SparkSQLContext,
    provider: Provider,
    connectionHandler: ConnectionHandler): IMetadataListener =
    MetadataListener {
      case updatedMetadata: TableMetadata =>
        timeFor(s"Received updated table metadata [$updatedMetadata]") {
          connectionHandler
            .getConnection(updatedMetadata.getClusterRef.getName)
            .foreach { connection =>
            registerTable(
              qualified(updatedMetadata.getName),
              sqlContext,
              provider,
              globalOptions(connection.config))
          }
        }
    } {
      case deletedMetadata: TableMetadata =>
        timeFor(s"Received deleted table metadata [$deletedMetadata]") {
          unregisterTable(
            qualified(deletedMetadata.getName),
            sqlContext)
        }
    }


}

/**
 * Helper for defining IMetadataListeners.
 * i.e.:{{{
 *   MetadataListener{
 *     metadataUpdated => //do whatever with updated metadata
 *   }{
 *     metadataDeleted => //do whatever with deleted metadata
 *   }
 * }}}
 */
object MetadataListener {

  type Callback = PartialFunction[IMetadata, Unit]

  def apply(
    onUpdate: Callback)(
    onDelete: Callback)(
    implicit logger: Logger): IMetadataListener =
    new IMetadataListener {
      def deleteMetadata(iMetadata: IMetadata): Unit =
        onUpdate.orElse(DoNothing("on delete"))(iMetadata)

      def updateMetadata(iMetadata: IMetadata): Unit =
        onDelete.orElse(DoNothing("on update"))(iMetadata)
    }

  private def DoNothing(when: String)(implicit logger: Logger): Callback = {
    case metadata => logger.debug(s"Event $metadata not recognised $when")
  }

}