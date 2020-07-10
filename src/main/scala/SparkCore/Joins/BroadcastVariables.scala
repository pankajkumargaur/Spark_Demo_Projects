package SparkCore.Joins

/**
  * Created by Pankaj Gaur on 10-07-2020.
  */

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import scala.io.Source
import scala.util.Try


object BroadcastVariables {

  def main(args: Array[String]) {

    //loadCSVFile("E:\\work\\Spark_Project_Repos\\Spark_Demo_Projects\\src\\main\\datasets\\countries.csv")

    loadCSVFile("src\\main\\datasets\\countries.csv") match {
      case Some(countries) => {
        val sc = SparkSession.builder.master("local[*]").appName("BroadcastVariables").getOrCreate()
          .sparkContext

        val countriesCache = sc.broadcast(countries)

        val countriesRDD = sc.parallelize(countries.keys.toList)

        // happy case...
        val happyCaseRDD = searchCountryDetails(countriesRDD, countriesCache, "A")
        println(">>>> Search results of countries starting with 'A': " + happyCaseRDD.count())
        happyCaseRDD.foreach(entry => println("Country:" + entry._1 + ", Capital:" + entry._2))

        // sad case...
        val sadCaseRDD = searchCountryDetails(countriesRDD, countriesCache, "Zz")
        println(">>>> Search results of countries starting with 'Zz': " + sadCaseRDD.count())
        sadCaseRDD.foreach(entry => println("Country:" + entry._1 + ", Capital:" + entry._2))
      }
      case None => println("Error loading file...")
    }

  }


  /**
    * Loads a CSV file from disk.
    * Returns a map as (key=country, value=capital).
    * Returns Some(map) on Success or None on Failure.
    */
  def loadCSVFile(filename: String): Option[Map[String, String]] = {
    val countries = Map[String, String]()

    Try {
      val bufferedSource = Source.fromFile(filename)
      for (line <- bufferedSource.getLines) {
        val Array(country, capital) = line.split(",").map(_.trim)
        countries += country -> capital
      }
      bufferedSource.close()
      return Some(countries)
    }.toOption
  }

  /**
    * Filters the input countries' RDD based on the search token and then
    * extracts their corresponding capitals from the broadcast variable.
    * Subsequently the searched countries and capitals are stored in a paired RDD
    * and returned to the caller.
    */
  def searchCountryDetails(countriesRDD: RDD[String], countryCache: Broadcast[Map[String, String]],
                           searchToken: String): RDD[(String, String)] = {
    countriesRDD.filter(_.startsWith(searchToken))
      .map(country => (country, countryCache.value(country)))
  }

}