import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Day15UDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day15-UDF")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n========== DAY 15: USER DEFINED FUNCTIONS ==========")

    import spark.implicits._

    // ------------------------------------------------
    // 1. Read employee data
    // ------------------------------------------------

    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n--- 1. ORIGINAL DATA ---")
    employees.show()

    // ------------------------------------------------
    // 2. Create a UDF to classify experience
    // ------------------------------------------------

    val experienceLevel = udf { experience: Int =>
      if (experience >= 5)
        "Senior"
      else if (experience >= 3)
        "Mid-Level"
      else
        "Junior"
    }

    println("\n--- 2. EXPERIENCE LEVEL UDF ---")

    employees
      .withColumn(
        "experience_level",
        experienceLevel($"experience")
      )
      .select(
        "employee_id",
        "name",
        "experience",
        "experience_level"
      )
      .show()

    // ------------------------------------------------
    // 3. Create a UDF to calculate annual salary
    // ------------------------------------------------

    val annualSalary = udf { monthlySalary: Int =>
      monthlySalary * 12
    }

    println("\n--- 3. ANNUAL SALARY UDF ---")

    employees
      .withColumn(
        "annual_salary",
        annualSalary($"salary")
      )
      .select(
        "name",
        "salary",
        "annual_salary"
      )
      .show()

    // ------------------------------------------------
    // 4. Create a UDF to classify salary
    // ------------------------------------------------

    val salaryCategory = udf { salary: Int =>
      if (salary >= 75000)
        "High"
      else if (salary >= 60000)
        "Medium"
      else
        "Low"
    }

    println("\n--- 4. SALARY CATEGORY UDF ---")

    employees
      .withColumn(
        "salary_category",
        salaryCategory($"salary")
      )
      .select(
        "name",
        "salary",
        "salary_category"
      )
      .show()

    // ------------------------------------------------
    // 5. Register UDF for Spark SQL
    // ------------------------------------------------

    spark.udf.register(
      "experience_level",
      (experience: Int) =>
        if (experience >= 5)
          "Senior"
        else if (experience >= 3)
          "Mid-Level"
        else
          "Junior"
    )

    employees.createOrReplaceTempView("employees")

    println("\n--- 5. UDF WITH SPARK SQL ---")

    spark.sql("""
      SELECT
        name,
        experience,
        experience_level(experience) AS experience_level
      FROM employees
      ORDER BY experience DESC
    """).show()

    // ------------------------------------------------
    // 6. Multiple UDFs together
    // ------------------------------------------------

    println("\n--- 6. MULTIPLE UDFS ---")

    employees
      .withColumn(
        "experience_level",
        experienceLevel($"experience")
      )
      .withColumn(
        "salary_category",
        salaryCategory($"salary")
      )
      .select(
        "name",
        "department",
        "salary",
        "experience",
        "experience_level",
        "salary_category"
      )
      .show()

    // ------------------------------------------------
    // 7. UDF explanation
    // ------------------------------------------------

    println("\n--- 7. UDF EXPLANATION ---")

    println("A UDF is a User Defined Function created for custom logic.")
    println("A UDF can be applied to DataFrame columns.")
    println("UDFs can also be registered and used inside Spark SQL queries.")
    println("UDFs are useful when built-in Spark functions do not provide")
    println("the required business logic.")

    println("\n========== DAY 15 COMPLETED SUCCESSFULLY ==========\n")

    spark.stop()
  }
}
