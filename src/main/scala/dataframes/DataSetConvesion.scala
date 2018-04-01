package dataframes

import org.apache.spark.sql.{SparkSession,DataFrame,Dataset}
import org.apache.spark.SparkConf

//
// Exploring interoperability between DataFrames and Datasets.
// Note that Dataset is covered in much greater detail in the 'dataset' directory.
//

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
final case class EmpSal(ename: String,
                        sal: Double)

object DataSetConvesion {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setAppName(getClass.getName)
                        .setMaster("local[2]")

    val spark = SparkSession
                  .builder()
                  .config(sparkConf)
                  .getOrCreate()

    import spark.implicits._

    // Lets instantiate 11 employee objects and create a sequence of employees
    val employees = Seq(
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

    // Create the DataFrame without passing through an RDD
    val employeeDF : DataFrame = spark.createDataFrame(employees)

    println("*** DataFrame schema")

    employeeDF.printSchema()

    println("*** DataFrame contents")

    employeeDF.show(5)

    println("*** Select and filter the DataFrame")

    val smallerDF =
      employeeDF.select("ename", "sal").filter($"deptno".equalTo(30))

    smallerDF.show()

    // Convert it to a Dataset by specifying the type of each row
    // by using a case class because we have one and it's most convenient to work with.
    // Notice you have to choose a case class that matches the remaining columns.
    // That's why we choose EmpSal case class instead of Employee class
    // Also notice that the columns keep their order as in the DataFrame

    val employeeDS: Dataset[EmpSal] = smallerDF.as[EmpSal]

    println("*** Dataset schema")

    employeeDS.printSchema()

    println("*** Dataset contents")

    employeeDS.show()

    // Select and other operations can be performed directly on a Dataset.
    // but there are "typed transformations", which produce a Dataset, and
    // "untyped transformations", which produce a DataFrame.
    // Project using a TypedColumn to gate a Dataset.
    val verySmallDS: Dataset[Double] = employeeDS.select($"sal".as[Double])

    println("*** Dataset after projecting one column")

    verySmallDS.show()

    // If you select multiple columns on a Dataset you will get a Dataset
    // of tuple type, and columns keep their names.
    val tupleDS : Dataset[(String, Double)] = employeeDS.select($"ename".as[String], $"sal".as[Double])

    println("*** Dataset after projecting two columns -- tuple version")

    tupleDS.show()

    // You can also cast back to a Dataset of a case class
    val betterDS: Dataset[EmpSal] = tupleDS.as[EmpSal]

    println("*** Dataset after projecting two columns -- case class version")

    betterDS.show()

    // Converting back to a DataFrame without making other changes is really easy
    val backToDataFrame : DataFrame = tupleDS.toDF()

    println("*** This time as a DataFrame")

    backToDataFrame.show()

    // While converting back to a DataFrame you can rename the columns
    val renamedDataFrame : DataFrame = tupleDS.toDF("MyState", "MySales")

    println("*** Again as a DataFrame but with renamed columns")

    renamedDataFrame.show()


  }
}
