package SparkSql

/**
  * Created by Pankaj Gaur on 13-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import org.apache.spark.sql.functions._


//SparkSql.CSV_Load_SQL

object CSV_Load_SQL {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("CSV_Load_SQL").getOrCreate()


   /* val df  = spark.read.format("com.databricks.spark.csv")
      .load("src/main/datasets/sales.csv")
*/

    val df = spark.read.format("csv")
     // .option("header", "true")
      .option("inferSchema", "true")
      //.option("delimiter" , ",")
      .load("src/main/datasets/sales.csv")
      .toDF("transactionId", "customerId","itemId","itemValue")

    //val df = spark.read.format("jdbc").load("src/main/datasets/sales.csv")

   // val df1 = spark.read.csv("src/main/datasets/sales.csv")



  /*  df.createOrReplaceTempView("mytable")
    val df_result = spark.sql("select transactionId, customerId, itemId from mytable where itemId = '222' ")

    df_result.show()

    +-------------+----------+------+
|transactionId|customerId|itemId|
+-------------+----------+------+
|          112|         2|   222|
|          115|         1|   222|
|          121|         1|   222|
|          124|         3|   222|
+-------------+----------+------+


{"transactionId":112,"customerId":2,"itemId":222}
{"transactionId":115,"customerId":1,"itemId":222}
{"transactionId":121,"customerId":1,"itemId":222}
{"transactionId":124,"customerId":3,"itemId":222}


*/

 /*   val df_result =  df.select(col("transactionId"),col("customerId"),col("itemId"))
      .filter(col("itemId") === "222")*/

    import spark.implicits._

    val df_result =  df.select(df("transactionId"),df("customerId"),df("itemId"))
      .filter(df("itemId") === "222")


  /*  val df_result1 =  df.select($"transactionId",$"customerId",$"itemId")
      .filter($"itemId" === "222")
*/



    df_result.write.format("parquet").save("src/main/Output/result")












  }
}