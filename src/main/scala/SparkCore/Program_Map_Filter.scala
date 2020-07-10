package SparkCore

/**
  * Created by Pankaj Gaur on 03-07-2020.
  *
  // Use case to load data from file  and remove header and take out only the record which has age > 21 and save result
  // Tranformaion used - Map  filter
  */

import org.apache.spark.sql.SparkSession

object Program_Map_Filter {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[1]")
      .appName("Example1")
      .getOrCreate()

    val sc = spark.sparkContext

    val myRDD = sc.textFile("src\\main\\datasets\\Customer.csv",5)

    // remove header from RDD
    val filteredRDD =  myRDD.filter(line => !line.startsWith("Id"))

    // take out 2nd  and 3rd element from array of string and filter age > 21
    val res = filteredRDD.map(rec =>  {
      val fields = rec.split(",")
      (fields(1),fields(2).toInt)
    }).filter(t => t._2 > 21)

    res.foreach(println)


    res.coalesce(1).saveAsTextFile("src\\main\\output\\result")

  }
}