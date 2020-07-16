package SparkSql

/**
  * Created by Pankaj Gaur on 16-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object jsondata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("jsondata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("jsondata").getOrCreate()

    val jsonDf = spark.read.format("json").load("src/main/datasets/companies.json")
    jsonDf.printSchema()
    jsonDf.show()


    val df  = jsonDf.select(col("_id.$oid").alias("id_oid") ,
      col("acquisition.acquired_day"),
      col("acquisition.acquired_month"),
      explode(col("competitions").alias("exp_competitions"))
    )

    import spark.implicits._

    df.select($"id_oid" ,$"acquired_day", $"acquired_month",$"col.competitor.name",
      $"col.competitor.permalink").coalesce(1)
      .write.format("csv")
      .save("src/main/Output/result1")





  }
}