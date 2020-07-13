package SparkCore

/**
  * Created by Pankaj Gaur on 11-07-2020.
  *
  * Ratings.csv file has the fields user, movieId and rating, timestamp.
    A given movie may get different ratings from different users. To get the average ratings of each movie,
    we need to add all ratings of each movie individually and divide the sum by the number of ratings.

	for example :

	moview Id 1, get 3 ratings
	Input  --
	// 1 , 0.5
  // 1,  0.2
	// 1,  0.2

	output should be
	1 , (0.5+0.2+0.2)/3

	MoviesID, AvgRatings
	1 , 0.3

  */

import org.apache.spark.sql.SparkSession


object AverageMovieRatings {

  def mapToTuple(line: String): (Int, (Float, Int)) = {
    val fields = line.split(',')
    (fields(1).toInt, (fields(2).toFloat, 1))

  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("AverageMovieRatings").getOrCreate()
    val sc = spark.sparkContext

    var data = sc.textFile("src\\main\\datasets\\ratings.csv")

    data.take(10).foreach(println)

    // Extract the first row which is the header
    val header = data.first();

    // Filter out the header from the dataset
    data = data.filter(row => row != header)

    val result = data.map(mapToTuple)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .sortBy(_._2, false)

    result.collect().foreach(println)

    // 1 , 0.5 , 1
    // 1, 0.1 ,  1

    // 1 0.6 2



    /// another use case find  -- Movie Rating Counter
   /* Another find the total movies based on ratings like -- group based on ratings and count
      0.5 ratings - 5 movies
      0.2  --- 10 - Movies
*/
    val result1 = data.map(line => line.split(',')(2).toFloat) // Extract rating from line as float
      .countByValue() // Count number of occurrences of each number

    // Sort and print the result
    result1.toSeq
      .sorted
      .foreach(println)







  }
}