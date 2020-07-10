package SparkCore

/**
  * Created by Pankaj Gaur on 10-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Accumalator {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Accumalator")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

   /* val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")

    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

    rdd.foreach(x => longAcc.add(x))
    println(longAcc.value)*/

    val dataRDD = sc.textFile("src\\main\\datasets\\sales-error.csv")
    val badRecords = sc.accumulator(0)

    dataRDD.foreach(record => {
      val parseResult = SalesDataParser.parse(record)
      if(parseResult.isLeft){

        badRecords += (1)
        println(badRecords)
      }
    })



    println("No of bad records is =  " + badRecords.value)



  }
}