package org.apache.spark.sql.cloud.fchen

import java.util.{Locale, Optional}

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, DataSource, LogicalRelation}
import org.apache.spark.sql.execution.streaming.{StreamingRelation, StreamingRelationV2}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, MicroBatchReadSupport}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.util.Utils

/**
 * @time 2019-07-08 21:49
 * @author fchen <cloud.chenfu@gmail.com>
 */
class ParserExtension(parserInterface: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan = {
    parserInterface.parsePlan(sqlText) match {
      case crvu: CreateTempViewUsing =>
        UnifyCreateTempViewUsing(crvu)
      case logicalPlan => logicalPlan
    }
  }

  override def parseExpression(sqlText: String): Expression = {
    parserInterface.parseExpression(sqlText)
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    parserInterface.parseTableIdentifier(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    parserInterface.parseFunctionIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    parserInterface.parseTableSchema(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    parserInterface.parseDataType(sqlText)
  }

}

case class UnifyCreateTempViewUsing(createTempViewUsing: CreateTempViewUsing) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (createTempViewUsing.provider.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, " +
        "you can't use it with CREATE TEMP VIEW USING")
    }

    val dataSource = DataSource(
      sparkSession,
      userSpecifiedSchema = createTempViewUsing.userSpecifiedSchema,
      className = createTempViewUsing.provider,
      options = createTempViewUsing.options)

    val ds = DataSource.lookupDataSource(
      createTempViewUsing.provider, sparkSession.sqlContext.conf).newInstance()
    val catalog = sparkSession.sessionState.catalog
    val viewDefinition = if (createTempViewUsing.options.contains("stream")) {
      val v1Relation = ds match {
        case _: StreamSourceProvider => Some(StreamingRelation(dataSource))
        case _ => None
      }
      ds match {
        case s: MicroBatchReadSupport =>
          val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
            ds = s, conf = sparkSession.sessionState.conf)
          val options = sessionOptions ++ createTempViewUsing.options
          val dataSourceOptions = new DataSourceOptions(options.asJava)
          var tempReader: MicroBatchReader = null
          val schema = try {
            tempReader = s.createMicroBatchReader(
              Optional.ofNullable(createTempViewUsing.userSpecifiedSchema.orNull),
              Utils.createTempDir(namePrefix = s"temporaryReader").getCanonicalPath,
              dataSourceOptions)
            tempReader.readSchema()
          } finally {
            // Stop tempReader to avoid side-effect thing
            if (tempReader != null) {
              tempReader.stop()
              tempReader = null
            }
          }
          StreamingRelationV2(
            s, createTempViewUsing.provider, options,
            schema.toAttributes, v1Relation)(sparkSession)
        case s: ContinuousReadSupport =>
          val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
            ds = s, conf = sparkSession.sessionState.conf)
          val options = sessionOptions ++ createTempViewUsing.options
          val dataSourceOptions = new DataSourceOptions(options.asJava)
          val tempReader = s.createContinuousReader(
            Optional.ofNullable(createTempViewUsing.userSpecifiedSchema.orNull),
            Utils.createTempDir(namePrefix = s"temporaryReader").getCanonicalPath,
            dataSourceOptions)
          StreamingRelationV2(
            s, createTempViewUsing.provider, options,
            tempReader.readSchema().toAttributes, v1Relation)(sparkSession)
        case _ =>
          // Code path for data source v1.
          StreamingRelation(dataSource)
      }
    } else {
      Dataset.ofRows(
        sparkSession, LogicalRelation(dataSource.resolveRelation())).logicalPlan
    }

    if (createTempViewUsing.global) {
      catalog.createGlobalTempView(createTempViewUsing.tableIdent.table, viewDefinition, createTempViewUsing.replace)
    } else {
      catalog.createTempView(createTempViewUsing.tableIdent.table, viewDefinition, createTempViewUsing.replace)
    }
    Seq.empty[Row]
  }
}
