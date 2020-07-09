package SparkCore

/**
  * Created by Pankaj Gaur on 06-07-2020.
  *
  * Use sales.csv data and get only the items which has itemID = 222 OR 333 data schema :
	transactionId, customerId, itemId, itemValue
  */

import org.apache.spark.sql.SparkSession

object Item_filter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Assignment_1_itemfilter").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("src//main//datasets//sales.csv").cache()

    val itemID1 = args(0)
    val itemID2 = args(1)


    //rdd.filter(line => line.contains("111") || line.contains("222") )

    rdd.filter(record => {
      val columns = record.split(",")
      val itemId = columns(2)

      if(itemId.equals(itemID1) || itemId == itemID2) true
      else false
    }).collect()foreach(println)


    rdd.count()






  }
}