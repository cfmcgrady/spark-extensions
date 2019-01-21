package cloud.fchen.spark.extensions.example

import java.util.UUID

import cloud.fchen.spark.utils.IdeaUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object IdeaRunTest {

  // scalastyle:off println
  def main(args: Array[String]): Unit = {

//    val uuid = UUID.randomUUID().toString.replaceAll("-", "")
    val uuid = this.getClass.getSimpleName.replaceAll("\\$", "")
    val classpathTempDir = s"/tmp/aaa/$uuid"
    val util = new IdeaUtil(
      None,
      Option(classpathTempDir),
      dependenciesInHDFSPath = s"libs/$uuid",
      principal = Option("chenfu@CDH.HOST.DXY"),
      keytab = Option("/Users/fchen/tmp/chenfu.keytab")
    )
    util.setup()
    val conf = new SparkConf()
      .setMaster("yarn-client")
      .set("spark.yarn.archive", "hdfs:///user/chenfu/libs/spark-2.4.0-bin-hadoop2.7.jar.zip")
      .set("spark.repl.class.outputDir", classpathTempDir)
    conf.getAll.foreach(println)
    println("-----")
    val spark = SparkSession
      .builder()
      .appName("Spark count example")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.parallelize(1 to 1000).map(i => {
      Hello(i)
    }).collect()
      .foreach(println)

    println("end")

  }

}

case class Hello(i: Int)
