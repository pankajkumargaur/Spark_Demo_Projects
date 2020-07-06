package SparkCore

/**
  * Created by Pankaj Gaur on 03-07-2020.
  */

import org.apache.spark.sql.SparkSession

object LoadData {
  def main(args: Array[String]) {

    val masterOfCluster = args(0) //local
    val inputPath = args(1) //src\main\datasets\transactions.csv

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .getOrCreate()

    val sc = sparkSession.sparkContext


    val transactionRdd = sc.textFile(inputPath,5)


    println(s"No of partitions is ${transactionRdd.getNumPartitions}")


    val result = transactionRdd.collect().toList

    /*just print 10 records*/
    result.take(10).foreach(println)

  }
}