import org.apache.spark.sql.SparkSession

object Day13SparkSQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day13-Spark-SQL-Basics")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n========== DAY 13: SPARK SQL BASICS ==========")

    // Read CSV file
    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n--- 1. EMPLOYEE DATA ---")
    employees.show()

    println("\n--- 2. SCHEMA ---")
    employees.printSchema()

    // Register DataFrame as temporary SQL view
    employees.createOrReplaceTempView("employees")

    println("\n--- 3. SQL: ALL EMPLOYEES ---")

    spark.sql("""
      SELECT *
      FROM employees
    """).show()

    println("\n--- 4. SQL: DATA ENGINEERING EMPLOYEES ---")

    spark.sql("""
      SELECT employee_id, name, salary
      FROM employees
      WHERE department = 'Data Engineering'
    """).show()

    println("\n--- 5. SQL: EMPLOYEES WITH SALARY > 65000 ---")

    spark.sql("""
      SELECT name, department, salary
      FROM employees
      WHERE salary > 65000
      ORDER BY salary DESC
    """).show()

    println("\n--- 6. SQL: DEPARTMENT-WISE EMPLOYEE COUNT ---")

    spark.sql("""
      SELECT department, COUNT(*) AS employee_count
      FROM employees
      GROUP BY department
      ORDER BY employee_count DESC
    """).show()

    println("\n--- 7. SQL: DEPARTMENT-WISE AVERAGE SALARY ---")

    spark.sql("""
      SELECT
        department,
        ROUND(AVG(salary), 2) AS average_salary
      FROM employees
      GROUP BY department
      ORDER BY average_salary DESC
    """).show()

    println("\n--- 8. SQL: HIGHEST PAID EMPLOYEE ---")

    spark.sql("""
      SELECT name, department, salary
      FROM employees
      ORDER BY salary DESC
      LIMIT 1
    """).show()

    println("\n--- 9. SQL: EXPERIENCED EMPLOYEES ---")

    spark.sql("""
      SELECT name, department, experience
      FROM employees
      WHERE experience >= 5
      ORDER BY experience DESC
    """).show()

    println("\n--- 10. SQL EXPLANATION ---")

    println("Spark SQL allows SQL queries to be executed on structured data.")
    println("A DataFrame can be registered as a temporary SQL view.")
    println("SQL operations such as SELECT, WHERE, GROUP BY, ORDER BY and")
    println("aggregate functions can then be executed using spark.sql().")

    println("\n========== DAY 13 COMPLETED SUCCESSFULLY ==========\n")

    spark.stop()
  }
}
