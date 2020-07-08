package SparkCore

/**
  * Created by Pankaj Gaur on 06-07-2020.
  * load data and tranform to first name, last name , fullName (first name, last name) and city
  * and provide schema using case class and then apply filter to get only record where city = WA
  */

import org.apache.spark.sql.SparkSession


case class Person(first_name:String,last_name:String, fullName:String, city:String )

object CaseClass {

  def parseRecord(record:String) ={
    val fields = record.split(",")
    val first_name = fields(0).replaceAll("\"", "")
    val last_name = fields(1).replaceAll("\"", "")
    val fullName = s"${first_name} ${last_name}"
    val city =  fields(5).replaceAll("\"", "")
    Person(first_name,last_name, fullName,city )

  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local[*]").appName("Assignment_2").getOrCreate()
    val sc = spark.sparkContext

    val csvrdd = sc.textFile("E:\\work\\datasets\\au-500.csv")

    val header = csvrdd.first()



   /* csvrdd.filter(record => record!= header)
    .map(record => {
      val fields = record.split(",")
      val first_name = fields(0).replaceAll("\"", "")
      val last_name = fields(1).replaceAll("\"", "")
      val fullName = s"${first_name} ${last_name}"
      val city =  fields(5).replaceAll("\"", "")
      (first_name,last_name, fullName,city )
    })
      .filter(record => record._4 == "WA")
      .collect().foreach(println)*/


    csvrdd.filter(record => record!= header).map(parseRecord)


  /*  val res = csvrdd.filter(record => record!= header)
      .map(record => {
        val fields = record.split(",")
        val first_name = fields(0).replaceAll("\"", "")
        val last_name = fields(1).replaceAll("\"", "")
        val fullName = s"${first_name} ${last_name}"
        val city =  fields(5).replaceAll("\"", "")
        Person(first_name,last_name, fullName,city )
      }).filter(person => person.city == "WA")

    import spark.implicits._

    res.toDF().show()*/


    //csvrdd.take(10).foreach(println)



  }
}