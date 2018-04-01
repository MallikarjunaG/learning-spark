package dataframes

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DateTime {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-DateTime")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("dt", DateType, true),
        StructField("ts", TimestampType, true)
      )
    )

    val data = Seq(
      "1, 2017-04-01 2017-04-01 11:30:00.123456",
      "2, 2017-03-31 2017-03-31 04:30:00.123456"
    "3, 2017-03-01 2017-03-01 11:30:00.453211"
    )

    val dataRDD = spark.sparkContext.parallelize(data)

    val df = dataRDD.map()

  }
}