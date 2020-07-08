package SparkCore

/**
  * Created by Pankaj Gaur on 06-07-2020.
  */

import org.apache.spark.sql.SparkSession

object ReducebyKey {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[*]")
      .appName("Example1")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.textFile("src//main//datasets//myfile.txt",5)

    println(rdd.getNumPartitions)


    val m = rdd.flatMap(line => {
      line.split(" ")
    }).map(word => (word,1))
      .filter(t=> ! t._1.startsWith("a"))
      .reduceByKey( (x,y) => x+y )
      .map(t => (t._2,t._1))
      .sortByKey(false)


    println(m.count())

    println(m.reduce((a, b) => (a._1 + b._1, a._2))._1)

    m.collect().foreach(println)


    rdd.collect().foreach(println)
    println(rdd.collect().mkString("~~~~~~~~~~~~~~~"))


    // map partitions eaxmples

    val numbers  = sc.parallelize(1 to 9, 3)

    numbers.map(x => {
      println("inside map function")
      (x, "hello")
    }).collect().foreach(println)

    numbers.mapPartitions(iter =>{
      println("Inside Map Partitions")
      (List(iter.next).iterator)
    }).collect().foreach(println)


    val rdd1 =  sc.parallelize(List("yellow",   "red", "blue",     "cyan", "black"), 3)

    val mapped =   rdd1.mapPartitionsWithIndex{
      // 'index' represents the Partition No
      // 'iterator' to iterate through all elements
      //  in the partition
      (index, iterator) => {
        println("Called in Partition -> " + index)
        val myList = iterator.toList
        // In a normal user case, we will do the
        // the initialization(ex : initializing database)
        // before iterating through each element
        myList.map(x => x + " -> " + index).iterator
      }
    }

    mapped.collect().foreach(println)

    // map partitions eaxmples



  }
}