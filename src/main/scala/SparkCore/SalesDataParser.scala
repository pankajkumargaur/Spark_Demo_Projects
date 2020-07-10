package SparkCore

/**
  * Created by Pankaj Gaur on 10-07-2020.
  */


case class SalesRecord(val transactionId: String,
                       val customerId: String,
                       val itemId: String,
                       val itemValue: Double)


object SalesDataParser {
  def parse(record:String): Either[(BadRecordException, String),SalesRecord] = {
    val columns = record.toString.split(",")
    if (columns.length == 4) {
      val transactionId: String = columns(0)
      val customerId: String = columns(1)
      val itemId: String = columns(2)
      val itemValue: Double = columns(3).toDouble
      Right(SalesRecord(transactionId, customerId, itemId, itemValue))
    }
    else {
      Left((new BadRecordException(), record))
    }
  }

  }
