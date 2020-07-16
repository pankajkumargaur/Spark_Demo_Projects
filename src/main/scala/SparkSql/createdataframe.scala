package SparkSql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._

/**
  * Created by Pankaj Gaur on 16-07-2020.
  */

case class emp  (name:String,  age:Int , city:String )

object createdataframe {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("createdataframe").getOrCreate()


    val sc = spark.sparkContext

    import spark.implicits._


    // 1
    val rdd = sc.textFile("src/main/datasets/asl.csv")
      .map(line => line.split(","))
      .map(rec => (rec(0),rec(1), rec(2)) ).toDF("name", "age", "city")


    //2 way

    val df = sc.textFile("src/main/datasets/asl.csv")
      .map(line => line.split(","))
      .map(rec => emp(rec(0),rec(1).toInt, rec(2)) ).toDF()


   // 3rd ways
   val schema1 = StructType(Array
   (StructField("name", StringType, true),
     StructField("age", IntegerType, true),
     StructField("city", StringType, true))
   )

    val rowRDD = sc.textFile("src/main/datasets/asl.csv")
      .map(line => line.split(","))
      .map(rec => Row(rec(0),rec(1).toInt, rec(2)) )


    val rdf = spark.createDataFrame(rowRDD,schema1 )

    rdf.show()

    // 4th ways

    val dfff  = spark.read.format("csv").schema(schema1).load("src/main/datasets/asl.csv")


  }





}
