package SparkCore

/**
  * Created by Pankaj Gaur on 11-07-2020.
  * From movies.csv dataset which has 3 coumns movieId,title,genres.
  * please get only the actons movies
  */

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}


object ActionMovies {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.master("local[*]").appName("ActionMovies").getOrCreate()
    val sc = spark.sparkContext

    // Read a text file
    var data = sc.textFile("src\\main\\datasets\\movies.csv")

    // Extract the first row which is the header
    val header = data.first();

    // Filter out the header from the dataset
    data = data.filter(row => row != header)

    val result = data.map(row => row.split(','))
      .map(fields => (fields(1), fields(2)))
      .flatMapValues(x => x.split('|'))
      .filter(x => x._2 == "Action")
      .map(x => x._1)
      .collect()

    result.sorted
      .foreach(println)



  }
}