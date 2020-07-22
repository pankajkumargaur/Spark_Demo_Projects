package SparkSql

/**
  * Created by Pankaj Gaur on 22-07-2020.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class Salary(depName: String, empNo: Long, salary: Long)

object Analytics_functions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Analytics_functions").getOrCreate()


    val empsalary = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200))

    import spark.implicits._

   /* val df  = spark.sparkContext.parallelize(empsalary).toDF()

    df.show()



    val byDepName =  Window.partitionBy("depName")

    df.withColumn("avg", avg("salary") over byDepName).show*/


    val tran_DF = spark.sparkContext.parallelize(Seq(
      (1,"src_trx1001",1,2000),
      (2,"src_trx1001",2,3000),
      (3,"src_trx1001",3,2500),

      (4,"src_trx1002",1,5000),
      (5,"src_trx1002",2,7000),
      (6,"src_trx1003",1,5000),
      (7,"src_trx1004",1,10000),
      (8,"src_trx1004",2,12000)
    )).toDF("ID","trx_src","version","trade_ammount")

    tran_DF.printSchema()
    tran_DF.show()

    // older approach
    val lat_versionDF = tran_DF.groupBy("trx_src").agg(max($"version").alias("latest_ver"))
    lat_versionDF.show


    tran_DF.join(lat_versionDF,tran_DF("version") === lat_versionDF("latest_ver") &&
      tran_DF("trx_src") === lat_versionDF("trx_src")
    ).orderBy(tran_DF("trx_src"))
      .toDF("ID","trx_src","version","trade_ammount","trx_src_dup","latest_ver")
      .drop("trx_src_dup").drop("version").show()


    // new approach


    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions._

    val windowSpec = Window.partitionBy("trx_src").orderBy(col("version").desc)
    tran_DF.withColumn("latest_Version", first("version").over(windowSpec)).
      select("*").where($"latest_Version" ===$"version").drop("latest_Version").orderBy($"trx_src").show()



  }


}