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
        |CREATE TABLE if not exists hdfsTable
        |(offset int, value string)
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '/tmp/test'
        |)
      """.stripMargin
    )
    spark.sql(
      """
        |CREATE TEMPORARY VIEW kafkaTable
        |USING kafka
        |OPTIONS (
        |  kafka.bootstrap.servers "localhost:9092",
        |  subscribe 'fchentest20190626',
        |  stream 'true'
        |)
      """.stripMargin
    )
    spark.sql(
      """
        |create or replace temporary view test_view as
        | select offset, (cast (value as string)) as value from kafkaTable
      """.stripMargin
    )
    spark.sql(
      """
        |select * from test_view
      """.stripMargin)
      .writeStream
      .format("console")
      .start()

    val x = spark.sql(
      """
        |insert into hdfsTable
        |  select * from test_view
      """.stripMargin
    )
    x.writeStream
      .start()
      .awaitTermination()

//    spark.sql("select * from kafkaTable")
//      .writeStream
//      .format("console")
//      .start()
//      .awaitTermination()
  }

}
