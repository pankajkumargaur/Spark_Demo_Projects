package SparkCore

/**
  * Created by Pankaj Gaur on 07-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object GroupbyKey {
  def main(args: Array[String]) {

    // below links shows the diffrence between reduceby key vs groupby key
    // https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html

    val spark = SparkSession.builder.master("local[*]").appName("GroupbyKey").getOrCreate()
    val sc = spark.sparkContext


/*
    val rdd = sc.textFile("src//main//datasets//myfile.txt")
    val words = rdd.flatMap(line => line.split(" "))
*/

    val words =  Array("one","two","two","four","five","six","six","eight","nine","ten","two","two")

    //  create RDD of array of strings
    val wordPairsRDD  = sc.parallelize(words)

    // group by example
    // groupByKey can cause out of disk problems as data is sent over the network and collected on the reduce workers.
    val data = wordPairsRDD.map(w=> (w,1))
      .groupByKey()
      .map(w => (w._1,w._2.sum))
    .sortBy(-_._2)


    data.collect().foreach(println)


    println("---------Reduce by key example -------------------")
    // reduce by key example
    //Data is combined at each partition , only one output for one key at each partition to send over network.
    // reduceByKey required combining all your values into another value with the exact same type.
    val data1 = wordPairsRDD.map(word => (word,1))
        .reduceByKey((x,y)=> x+y)

    data.collect().foreach(println)


  }
}