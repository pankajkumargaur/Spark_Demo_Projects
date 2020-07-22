package SparkSql

/**
  * Created by Pankaj Gaur on 21-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object data {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("data").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("data").getOrCreate()




  }
}