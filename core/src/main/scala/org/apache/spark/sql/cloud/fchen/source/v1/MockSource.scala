package org.apache.spark.sql.cloud.fchen.source.v1

import scala.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class MockSource(sqlContext: SQLContext,
                 sourceOptions: Map[String, String],
                 metadataPath: String) extends Source with Logging {
  val df = sqlContext.sparkSession.read.json(sourceOptions("path"))
  val loadAll = sourceOptions.getOrElse("loadAll", "false").toBoolean
  val dataWithOffset = df.rdd.collect().map(r => {
    InternalRow.fromSeq(
      schema.fields.map(s => {
        if (s.dataType.isInstanceOf[StringType]) {
          UTF8String.fromString(r.getString(r.fieldIndex(s.name)))
        } else {
          r.get(r.fieldIndex(s.name))
        }
      })
    )
  }).zipWithIndex

  var currentOffset: Long = 0L

  override def schema: StructType = {
    df.schema
  }

  override def getOffset: Option[Offset] = {
    currentOffset += Random.nextInt(10)
    Option(LongOffset(currentOffset))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val ds = {
      if (loadAll) {
        dataWithOffset.map(_._1)
      } else {
        val from = if (start.isDefined) {
          start.get.asInstanceOf[LongOffset].offset
        } else {
          -1
        }
        val to = end.asInstanceOf[LongOffset].offset
        logInfo(s"get data from ${from} to ${to}")
        dataWithOffset.filter {
          case (_, index) =>
            index > from && index <= to
        }.map(_._1)
      }
    }
    //    sqlContext.sparkSession.createDataFrame(ds.toList.asJava, schema)

    //    sqlContext.createDataFrame(df.rdd, schema)
    //    sparkSession.internalCreateDataFrame(parsed, schema, isStreaming = csvDataset.isStreaming)
    sqlContext.internalCreateDataFrame(
      sqlContext.sparkContext.parallelize(ds), schema, isStreaming = true)
//      sqlContext.sparkContext.parallelize(ds), schema)

  }
  override def stop(): Unit = {

  }
}

class MockStreamSourceProvider extends DataSourceRegister with StreamSourceProvider with Logging {
  override def shortName(): String = "mockStream"

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    val df = sqlContext.sparkSession.read.json(parameters("path"))
    (shortName(), df.schema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): Source = {
    new MockSource(sqlContext, parameters, metadataPath)
  }
}
