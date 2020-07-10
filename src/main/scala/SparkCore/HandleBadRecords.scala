package SparkCore

/**
  * Created by Pankaj Gaur on 10-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

object HandleBadRecords {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("HandleBadRecords")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataRDD = sc.textFile("src\\main\\datasets\\sales-error.csv")


    val parsedData = dataRDD.map(SalesDataParser.parse).persist(StorageLevel.MEMORY_ONLY_SER)




    val malformedRecords = parsedData.filter(x => x.isLeft).map(x=> x.left.get._2).cache()

    //val malformedRecords = parsedData.filter(_.isLeft).map(_.left.get._2).cache()

    //val goodRecords = parsedData.filter(_.isRight).map(_.right.get)

    val goodRecords = parsedData.filter(x=> x.isRight).map(x => x.right.get)

    malformedRecords.collect().foreach(println)
    goodRecords.collect().foreach(println)

    goodRecords.filter(s => s.itemId == "333").collect().foreach(println)





  }
}