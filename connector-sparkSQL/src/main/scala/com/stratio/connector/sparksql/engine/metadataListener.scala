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
import com.stratio.connector.sparksql.providers
import com.stratio.connector.sparksql.SparkSQLContext
import com.stratio.crossdata.common.connector.IMetadataListener
import com.stratio.connector.commons.{Loggable, Metrics}
import com.stratio.crossdata.common.data.Name
import com.stratio.crossdata.common.metadata.{UpdatableMetadata, TableMetadata}
import com.stratio.crossdata.common.statements.structures.{Selector, StringSelector}
import org.slf4j.Logger
import com.stratio.connector.sparksql.engine.query.QueryEngine._
import scala.collection.JavaConversions._

/**
 * Hook for receiving metadata update events.
 */
object SparkSQLMetadataListener extends Loggable with Metrics {

  import timer._

  def apply(
    sqlContext: SparkSQLContext,
    connectionHandler: ConnectionHandler): IMetadataListener =
    MetadataListener {
      case updatedMetadata: TableMetadata =>
        logger.info(s"Received updated table metadata [$updatedMetadata]")
        timeFor("Received updated table metadata.") {
          for {
            connection <- connectionHandler.getConnection(updatedMetadata.getClusterRef.getName)
            provider <- providers.apply(connection.config.getDataStoreName.getName)
          } registerTable(
            qualified(updatedMetadata.getName),
            sqlContext,
            provider,
            globalOptions(connection.config) ++ updatedMetadata.getOptions.toMap.map{
              case (k,v) => k.getStringValue -> v.getStringValue
            })
        }
      case other: UpdatableMetadata =>
        logger.debug(s"'$other'[${other.getClass}] has no callbacks associated...")
    } {
      case deletedMetadata: Name =>
        logger.info(s"Received deleted table metadata [$deletedMetadata]")
        timeFor("Received deleted table metadata") {
          unregisterTable(
            deletedMetadata.getQualifiedName,
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

  type Callback[T] = PartialFunction[T, Unit]

  def apply(
    onUpdate: Callback[UpdatableMetadata])(
    onDelete: Callback[Name])(
    implicit logger: Logger): IMetadataListener =
    new IMetadataListener {

      override def updateMetadata(uMetadata: UpdatableMetadata): Unit =
        onUpdate.orElse(DoNothing[UpdatableMetadata]("on delete"))(uMetadata)

      override def deleteMetadata(uName: Name): Unit =
        onDelete.orElse(DoNothing[Name]("on update"))(uName)

    }

  private def DoNothing[T](when: String)(implicit logger: Logger): Callback[T] = {
    case metadata => logger.debug(s"Event $metadata not recognised $when")
  }

}