package Spark_Streaming

/**
  * Created by Pankaj Gaur on 14-07-2020.
  */

import org.apache.spark.sql.SparkSession


object test {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    import spark.implicits._

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = lines.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()




  }
}