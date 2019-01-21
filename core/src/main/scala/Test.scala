import scala.util.Random

import cloud.fchen.spark.utils.IdeaUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by fchen on 2017/9/11.
 */
object Test{
  val count = 100
  def main(args: Array[String]): Unit = {
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
    // scalastyle:off println
    val conf = new SparkConf()
      .setMaster("yarn-client")
      .set("spark.yarn.archive", "hdfs:///user/chenfu/libs/spark-2.4.0-bin-hadoop2.7.jar.zip")
      .set("spark.repl.class.outputDir", classpathTempDir)
      .set("spark.executor.instances", "100")
    val spark = SparkSession
      .builder()
      .appName("Spark count example")
      .config(conf)
      .getOrCreate()

    println(spark.sparkContext.parallelize(1 to 1000).count())
    val nodes = vertex.toArray
    println("============")
    nodes.foreach(println)
    println("============")
    var result = spark.sparkContext
      .parallelize(Array(Point(0, 0)))
      .flatMap(n => {
        nodes.map(nn => {
          Array(n, nn)
        })
      })
      .flatMap(n => {
        nodes.diff(n)
          .map(nn => {
            n :+ nn
          })
      }).repartition(100)
    (1 to nodes.size - 2).foreach(i => {

      result = result.flatMap(n => {
        nodes.diff(n)
          .map(nn => {
            n :+ nn
          })
      })
      if (i % 20 == 0) {
        result = result.repartition(100)
      }
    })


    val fr = result.map(x => {
      var sum = 0d
      (0 until x.size - 1).foreach(index => {
        sum += x(index).dist(x(index + 1))
      })
      (sum, x)
    }).min()(Ordering.by(_._1))
    println(fr._1)
    println(fr._2.mkString(","))
    //      .collect()

    //    result.foreach(x => {
    //      println(x.mkString(","))
    //    })
  }

  def vertex: Set[Point] = {
    (1 to count).map(i => {
      val x = Random.nextInt(100)
      val y = Random.nextInt(100)
      Point(x, y)
    }).toSet
  }
}
case class Point(x: Int, y: Int) {
  def dist(that: Point): Double = {
    val xx = that.x - x
    val yy = that.y - y
    xx * xx + yy * yy
  }
}
