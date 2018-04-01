package dataframes

import org.apache.spark.sql.SparkSession

// Define a case class
final case class Employee(empno: Int,
                          ename: String,
                          job: String,
                          mgr: Int,
                          hiredate: String,
                          sal: Double,
                          comm: Double,
                          deptno: Int
                         )

object Basics {

  // main method
  def main(arguments: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("DataFrame Basics")
        .master("local[2]")
        .getOrCreate()

    import spark.implicits._

    // Lets instantiate 11 employee objects and create a sequence of employees
    val empSeq = Seq(
      Employee(7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      Employee(7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      Employee(7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      Employee(7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      Employee(7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      Employee(7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      Employee(7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      Employee(7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      Employee(7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      Employee(7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      Employee(7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )

    // Method 1: Creating Dataframe directly from a Seq of Objects
    val empDfFromSeq = spark.createDataFrame(empSeq)

    // Method 2: Create an RDD from Seq of Objects with desired parallelism and then create a DataFrame
    val empRDD = spark.sparkContext.parallelize(empSeq,4)

    val empDfFromRDD = spark.createDataFrame(empRDD)

    println(s"*** no of partitions in empDfFromSeq ${empDfFromSeq.rdd.getNumPartitions}")

    println(s"*** no of partitions in empDfFromSeq ${empDfFromRDD.rdd.getNumPartitions}")

    println("*** display first two rows from the dataFrame in Array[Row] format")

    empDfFromRDD head 2 foreach println

    println("*** toString() just gives you the schema")

    println(empDfFromSeq.toString())

    println("*** columns just gives you the fields")

    println(empDfFromSeq.columns.foreach(println))

    println("*** It's better to use printSchema()")

    empDfFromSeq.printSchema()

    println("*** show() gives you neatly formatted data")

    empDfFromSeq.show()

    println("*** use show(n) to display n rows")

    empDfFromSeq.show(2)

    println("*** use select(n,truncate=false) to display n rows and to display column contents without truncation")

    empDfFromSeq.show(2, truncate = false)

    println("*** use select() to choose one column")

    empDfFromSeq.select("empno").show()

    println("*** use select() for multiple columns")

    empDfFromSeq.select("ename", "sal").show()

    println("*** use filter() to choose rows")

    empDfFromSeq.filter($"job".equalTo("MANAGER")).show()

    println("*** use where() to choose rows, same as filter()")

    empDfFromSeq.where($"job".equalTo("MANAGER")).show()

    println("*** ADVANCED: using multiple conditions in where() to choose rows")

    empDfFromSeq
      .where($"job".equalTo("MANAGER")
        .and($"deptno".isin(10,20))
        .and($"mgr".equalTo(7839))).show()

    println("*** use as() or alias to rename columns")

    empDfFromSeq.select($"ename".as("EmpName"), $"empno".alias("EmpNo")).show()

    // Ways to select columns
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.functions.column

    println("*** Select list containing Column Names of type Column")
    empDfFromSeq.select(column("empno"), col("ename"), empDfFromSeq.col("job"), $"sal").show(2)

    println("*** Select list with Column Names of type String")
    empDfFromSeq.select("empno", "ename", "mgr").show(2)

    // Ways to Select Seq of Columns
    val fieldList = List("ename", "sal", "comm", "job")

    println("*** Select list with Seq of Column Names of type String")
    empDfFromSeq.select(fieldList.head, fieldList.tail :_*).show(2)

    // Define a List[Column] from List[String]
    val fieldListOfCol = fieldList.map(col) // same as fieldList.map(x => col(x))

    println("*** Select list with Seq of Column Names of type Column")
    empDfFromSeq.select(fieldListOfCol :_*).show(2)

  }

}
