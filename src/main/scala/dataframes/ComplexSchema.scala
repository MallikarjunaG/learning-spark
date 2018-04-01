package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, ArrayType, MapType, StructField, IntegerType, StringType, DoubleType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{explode, size, array_contains,array}

object ComplexSchema {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrames ComplexSchemaUsingStructType")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    //
    // Example 1: Nested StructType for Nested Rows
    //

    val studentMarks = Seq(
      Row(1, Row("john", "doe"), 6, Row(70.0, 45.0, 85.0)),
      Row(2, Row("jane", "doe"), 9, Row(80.0, 35.0, 92.5))
    )

    val studentMarksRDD = spark.sparkContext.parallelize(studentMarks, 4)

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StructType(
          Seq(
            StructField("first", StringType, nullable = true),
            StructField("last", StringType, nullable = true)
          )
        ), nullable = true),
        StructField("standard", IntegerType, nullable = true),
        StructField("marks", StructType(
          Seq(
            StructField("maths", DoubleType, nullable = true),
            StructField("physics", DoubleType, nullable = true),
            StructField("chemistry", DoubleType, nullable = true)
          )
        ), nullable = true)
      )
    )

    println(s"Position of subfield 'standard' is ${schema.fieldIndex("standard")}")

    val studentMarksDF = spark.createDataFrame(studentMarksRDD, schema)

    println("Schema with nested struct")
    studentMarksDF.printSchema()

    println("DataFrame with nested Row")
    studentMarksDF.show()

    println("Select the column with nested Row at the top level")
    studentMarksDF.select("name").show()

    println("Select deep into the column with nested Row")
    studentMarksDF.select("name.first").show()

    println("The column function getField() seems to be the 'right' way")
    studentMarksDF.select($"name".getField("last")).show()

    //
    // Example 2: ArrayType
    //

    val studentMarks2 = Seq(
      Row(1, Row("john", "doe"), 6, Array(70.0, 35.0, 85.0)),
      Row(2, Row("jane", "doe"), 9, Array(80.0, 35.0, 92.5, 35.0, 46.0))
    )
    val studentMarks2Rdd = spark.sparkContext.parallelize(studentMarks2, 4)

    val schema2 =
      (new StructType)
        .add("id", "int", nullable = true)
        .add("name", (new StructType)
          .add("first", "string", nullable = true)
          .add("last", "string", nullable = true),nullable = true)
        .add("standard", "int", true)
        .add("marks", new ArrayType(DoubleType,containsNull = false),nullable = true)

    val studentMarks2DF = spark.createDataFrame(studentMarks2Rdd, schema2)

    println("Schema with array")
    studentMarks2DF.printSchema()

    println("DataFrame with array")
    studentMarks2DF.show()

    println("Count elements of each array in the column")
    studentMarks2DF.select($"id", size($"marks").as("count")).show()

    println("Explode the array elements out into additional rows")
    studentMarks2DF.select($"id", explode($"marks").as("scores")).show()

    println("Apply a membership test to each array in a column")
    studentMarks2DF.select($"id", array_contains($"marks", 35.0).as("has35")).show()

    studentMarks2DF.selectExpr("id", "marks[2]").show()

    studentMarks2DF.select($"id", $"marks"(1)).show()

    println("Use column function getItem() to index into array when selecting")
    studentMarks2DF.select($"id", $"marks".getItem(3)).show()

    println("Select first 3 elements from the Marks array as an Array")
    studentMarks2DF.select($"id", $"name.first", array((0 until 3).map($"marks"(_)) :_*)).show()

    ///
    // Example 3: MapType
    //

    val studentMarks3 = Seq(
      Row(1, Row("john", "doe"), 6, Map("m" -> 70.0, "p" -> 35.0, "c" -> 85.0)),
      Row(2, Row("jane", "doe"), 9, Map(("m", 80.0), ("p",35.0), ("c", 92.5), ("s", 35.0), ("e", 46.0)))
    )

    val studentMarks3RDD = spark.sparkContext.parallelize(studentMarks3, 4)

    val schema3 =
      StructType(
        StructField("id", IntegerType, true)::
        StructField("name", StructType(
            StructField("first", StringType)::
            StructField("last", StringType)::Nil
        ), true)::
        StructField("standard", IntegerType, true)::
        StructField("marks", MapType(StringType, DoubleType))::Nil
      )

    val studentMarks3DF = spark.createDataFrame(studentMarks3RDD, schema3)

    println("Schema with map")
    studentMarks3DF.printSchema()

    println("DataFrame with map")
    studentMarks3DF.show()

    println("Count elements of each map in the column")
    studentMarks3DF.select($"id", size($"marks").as("count")).show()

    // notice you get one column from the keys and one from the values
    println("Explode the map elements out into additional rows")
    studentMarks3DF.select($"id", explode($"m")).show()

    // MapType is actually a more flexible version of StructType, since you
    // can select down into fields within a column, and the rows where
    // an element is missing just return a null
    println("Select deep into the column with a Map")
    studentMarks3DF.select($"id", $"marks.e").show()

    println("The column function getItem() seems to be the 'right' way")
    studentMarks3DF.select($"id", $"marks".getItem("p")).show()
  }
}
