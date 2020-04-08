package com.bigdata.spark

/**
  * Created by Pankaj Gaur on 08-04-2020.
  */

import org.apache.spark.sql.SparkSession

object Example1 {
  def main(args: Array[String]) {

    val spark =  SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val df  = spark.read.format("csv").load("E:\\work\\datasets\\au-500.csv")
    df.printSchema()
    df.show()

  }
}