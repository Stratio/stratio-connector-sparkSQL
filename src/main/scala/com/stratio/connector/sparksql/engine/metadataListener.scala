package com.stratio.connector.sparksql.engine

import com.stratio.connector.commons.timer
import com.stratio.connector.sparksql.{Loggable, Provider, SparkSQLContext}
import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IMetadataListener}
import com.stratio.crossdata.common.data.TableName
import com.stratio.crossdata.common.metadata.{TableMetadata, IMetadata}
import scala.collection.JavaConversions._

/**
 * Hook for receiving metadata update events.
 */
object SparkSQLMetadataListener extends Loggable {

  import timer._

  def apply(
    sqlContext: SparkSQLContext,
    provider: Provider,
    config: ConnectorClusterConfig): IMetadataListener =
    MetadataListener {
      case updatedMetadata: TableMetadata =>
        time(s"Received updated table metadata [$updatedMetadata]") {
          registerTable(
            qualified(updatedMetadata.getName),
            sqlContext,
            provider,
            config.getClusterOptions.toMap ++
              config.getConnectorOptions.toMap)
        }
    } {
      case deletedMetadata: TableMetadata =>
        time(s"Received deleted table metadata [$deletedMetadata]") {
          unregisterTable(
            qualified(deletedMetadata.getName),
            sqlContext)
        }
    }

  //  Converts name to canonical format.
  private def qualified(name: TableName): Seq[String] =
    name.getQualifiedName.split("\\.")

  /*
   * Register a table with its options in sqlContext catalog.
   * If table already exists, it throws a warning.
   */
  private def registerTable(
    tableName: Seq[String],
    sqlContext: SparkSQLContext,
    provider: Provider,
    options: Map[String, String]): Unit = {
    if (sqlContext.getCatalog.tableExists(tableName))
      logger.warn(s"Tried to register ${tableName.mkString(".")} table " +
        s"but it already exists!")
    else {
      logger.debug(s"Registering table [$tableName]")
      sqlContext.sql(createTemporaryTable(
        tableName.mkString("."),
        provider,
        options))
    }
  }

  /*
   * Unregister, if exists, given table name.
   */
  private def unregisterTable(
    tableName: Seq[String],
    sqlContext: SparkSQLContext): Unit = {
    if (!sqlContext.getCatalog.tableExists(tableName))
      logger.warn(s"Tried to unregister ${tableName.mkString(".")} table " +
        s"but it already exists!")
    else {
      logger.debug(s"Unregistering table [${tableName.mkString(".")}]")
      sqlContext.getCatalog.unregisterTable(tableName)
    }
  }

  /*
   *  Provides the necessary syntax for creating a temporary table in SparkSQL.
   */
  private def createTemporaryTable(
    table: String,
    provider: Provider,
    options: Map[String, String]): String =
    s"""
       |CREATE TEMPORARY TABLE $table
        |USING $provider
        |OPTIONS (${options.map { case (k, v) => s"$k '$v'"}.mkString(",")})
       """.stripMargin

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

  def apply(onUpdate: Callback)(onDelete: Callback): IMetadataListener =
    new IMetadataListener {
      def deleteMetadata(iMetadata: IMetadata): Unit =
        onUpdate.orElse(DoNothing)(iMetadata)

      def updateMetadata(iMetadata: IMetadata): Unit =
        onDelete.orElse(DoNothing)(iMetadata)
    }

  private val DoNothing: Callback = {
    case _ => ()
  }

}
