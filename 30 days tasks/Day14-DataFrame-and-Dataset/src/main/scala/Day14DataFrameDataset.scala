import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders

case class Employee(
  employee_id: String,
  name: String,
  department: String,
  salary: Int,
  experience: Int
)

object Day14DataFrameDataset {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day14-DataFrame-and-Dataset")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("\n========== DAY 14: DATAFRAME AND DATASET ==========")

    // ------------------------------------------------
    // 1. Read CSV as DataFrame
    // ------------------------------------------------

    println("\n--- 1. DATAFRAME ---")

    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    employeeDF.show()

    println("\nDataFrame Schema:")
    employeeDF.printSchema()

    // ------------------------------------------------
    // 2. DataFrame operations
    // ------------------------------------------------

    println("\n--- 2. DATAFRAME OPERATIONS ---")

    employeeDF
      .select("name", "department", "salary")
      .show()

    println("\nEmployees with salary > 65000:")

    employeeDF
      .filter($"salary" > 65000)
      .show()

    // ------------------------------------------------
    // 3. Convert DataFrame to Dataset
    // ------------------------------------------------

    println("\n--- 3. DATAFRAME TO DATASET ---")

    val employeeDS: Dataset[Employee] =
      employeeDF.as[Employee]

    employeeDS.show()

    println("\nDataset Schema:")
    employeeDS.printSchema()

    // ------------------------------------------------
    // 4. Dataset typed operations
    // ------------------------------------------------

    println("\n--- 4. DATASET TYPED OPERATIONS ---")

    val experiencedEmployees =
      employeeDS.filter(_.experience >= 5)

    experiencedEmployees.show()

    println("\nExperienced employee names:")

    experiencedEmployees
      .map(_.name)
      .collect()
      .foreach(println)

    // ------------------------------------------------
    // 5. Dataset transformation
    // ------------------------------------------------

    println("\n--- 5. DATASET TRANSFORMATION ---")

    val salaryIncrease =
      employeeDS.map { employee =>
        employee.copy(
          salary = (employee.salary * 1.10).toInt
        )
      }

    salaryIncrease.show()

    // ------------------------------------------------
    // 6. DataFrame vs Dataset
    // ------------------------------------------------

    println("\n--- 6. DATAFRAME VS DATASET ---")

    println("DataFrame:")
    println("- Distributed collection organized into named columns.")
    println("- Provides SQL-style and untyped operations.")
    println("- Convenient for structured data processing.")

    println("\nDataset:")
    println("- Provides compile-time type safety.")
    println("- Uses case classes for typed records.")
    println("- Supports typed transformations such as map and filter.")

    println("\n--- 7. KEY DIFFERENCE ---")

    println("DataFrame = Dataset[Row]")
    println("Dataset = strongly typed structured collection.")
    println("DataFrames are generally convenient for SQL and column operations.")
    println("Datasets provide stronger compile-time type safety.")

    println("\n========== DAY 14 COMPLETED SUCCESSFULLY ==========\n")

    spark.stop()
  }
}
