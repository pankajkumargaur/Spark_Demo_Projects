package SparkCore

/**
  * Created by Pankaj Gaur on 08-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object ItemwiseCount {

  def getTuple(record: String) ={
    val fields = record.split(",")
    (fields(2), 1)
  }


  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ItemwiseCount").getOrCreate()
    val sc = spark.sparkContext

    val dataRDD = sc.textFile("src\\main\\datasets\\sales.csv")

    dataRDD.collect().foreach(println)


    val itemPair = dataRDD.map(record => {
      val fields = record.split(",")
      ((fields(2)), 1)
    })

    //val itemPair = dataRDD.map(getTuple)

    val resultRdd = itemPair.reduceByKey((x, y) => x + y).sortBy(- _._2)

    println(resultRdd.collect().toList)

    resultRdd.saveAsTextFile("src\\main\\Output\\result")
  }
}