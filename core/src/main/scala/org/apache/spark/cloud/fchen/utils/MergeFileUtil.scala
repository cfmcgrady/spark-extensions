package org.apache.spark.cloud.fchen.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.Utils

/**
 * @time 2019-03-11 14:42
 * @author fchen <cloud.chenfu@gmail.com>
 */
object MergeFileUtil {

  def main(args: Array[String]): Unit = {

    if (args.size != 2) {
      // scalastyle:off println
      println("usage: [source directory], [number of partition]")
      // scalastyle:on
      System.exit(0)
    }
    val srcDir = args(0)
    val numPar = args(1).toInt
    println(s"merge source directory: [ $srcDir ], number of partitions: [ $numPar ].")
    val spark = SparkSession
      .builder()
      .appName("Spark count example")
      .master("local[4]")
      .getOrCreate()
    merge(spark, srcDir, numPar)
  }

  def merge(spark: SparkSession, srcDir: String, numPar: Int = 20): Unit = {
    val disDir = s"/tmp/merge${srcDir}"
    spark.read
      .parquet(srcDir)
      .coalesce(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(disDir)
    val p = Utils.getHadoopFileSystem("/", spark.sparkContext.hadoopConfiguration)
    val oldPartition = new Path(srcDir)
    p.delete(oldPartition, true)
    p.rename(new Path(disDir), new Path(srcDir))
  }

}
