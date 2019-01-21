import org.apache.spark.sql.SparkSession

/**
 * Created by fchen on 2017/9/11.
 */
object Test{
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    val spark = SparkSession
      .builder()
      .appName("Spark count example")
      .master("local[4]")
      .getOrCreate()

    println(spark.sparkContext.parallelize(1 to 1000).count())

  }
}
