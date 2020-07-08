package SparkCore.Joins

/**
  * Created by Pankaj Gaur on 08-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Joins_Example2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Joins_Example2").getOrCreate()
    val sc = spark.sparkContext


    val data1 = "E:\\work\\datasets\\asl.csv"
    val data2 = "E:\\work\\datasets\\nep.csv"

    val aslrdd = sc.textFile(data1)
    val neprdd1 = sc.textFile(data2)

    val head =  neprdd1.first()
    val neprdd = neprdd1.filter(x=> x!=head)

    val asl = aslrdd.map(x=> x.split(",")).map(x=> (x(0),(x(1),x(2)) )   )
    val nep = neprdd.map(x=> x.split(",")).map(x=> (x(0),(x(1),x(2))))

    println(asl.collect().toList)
    println(nep.collect().toList)

    asl.join(nep).collect().foreach(println)

  /*  val data = spark.sparkContext.parallelize(Array(('A',1),('b',2),('c',3)))
    val data2 =spark.sparkContext.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val result = data.join(data2)
    println(result.collect().mkString(","))
    */






  }
}