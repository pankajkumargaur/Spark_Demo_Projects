package SparkCore

/**
  * Created by Pankaj Gaur on 03-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object CreateRDD {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[1]")
      .appName("CreateRDD")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(List(1,2,3,4,5))

    val rddCollect = rdd.collect()

    println("Number of Partitions: "+rdd.getNumPartitions)
    println("Action: First element: "+rdd.first())
    println("Action: RDD converted to Array[Int] : ")

    rddCollect.foreach(println)

  }
}