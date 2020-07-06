package SparkCore

/**
  * Created by Pankaj Gaur on 03-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Example1 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[1]")
      .appName("Example1")
      .getOrCreate()

    val sc = spark.sparkContext

    val myRDD = sc.textFile("src\\main\\datasets\\Customer.csv")

    val filteredRDD =  myRDD.filter(line => !line.startsWith("Id"))

    val res = filteredRDD.map(rec =>  {
      val fields = rec.split(",")
      (fields(1),fields(2).toInt)
    }).filter(t => t._2 > 21)

    res.foreach(println)

    res.saveAsTextFile("E:\\work\\datasets\\mytargetoutput")

  }
}