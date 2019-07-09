package cloud.fchen.spark.extensions.example

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.cloud.fchen.ParserExtension

/**
 * @time 2019-07-09 10:58
 * @author fchen <cloud.chenfu@gmail.com>
 */
object StreamSQL {
  def main(args: Array[String]): Unit = {

    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    //  val parserBuilder: ParserBuilder = (spark, parser) => new MySqlParser(spark.sessionState.conf)
    val parserBuilder: ParserBuilder = (spark, parser) => new ParserExtension(parser)
    val extBuilder: ExtensionsBuilder = {
      e =>
        e.injectParser(parserBuilder)
    }
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .withExtensions(extBuilder)
      .getOrCreate()
    spark.sql(
      """
        |CREATE TEMPORARY VIEW kafkaTable
        |USING kafka
        |OPTIONS (
        |  kafka.bootstrap.servers "n1.cdh.host.dxy:9092,n2.cdh.host.dxy:9092,n5.cdh.host.dxy:9092",
        |  subscribe 'fchentest20190626',
        |  stream 'true'
        |)
      """.stripMargin
    ).explain(true)
    spark.sql("select * from kafkaTable")
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }

}
