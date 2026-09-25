import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day18Joins {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18 Joins")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n========== DAY 18: JOINS ==========\n")

    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    val departments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/departments.csv")

    println("---------- EMPLOYEES ----------")
    employees.show(false)

    println("---------- DEPARTMENTS ----------")
    departments.show(false)

    val joinCondition =
      employees("department_id") === departments("department_id")

    // 1. INNER JOIN
    println("---------- 1. INNER JOIN ----------")

    val innerJoin = employees
      .join(departments, joinCondition, "inner")
      .select(
        employees("employee_id"),
        employees("employee_name"),
        employees("department_id"),
        departments("department_name"),
        departments("location"),
        employees("salary")
      )

    innerJoin.show(false)

    // 2. LEFT JOIN
    println("---------- 2. LEFT JOIN ----------")

    val leftJoin = employees
      .join(departments, joinCondition, "left")
      .select(
        employees("employee_id"),
        employees("employee_name"),
        employees("department_id"),
        departments("department_name"),
        departments("location"),
        employees("salary")
      )

    leftJoin.show(false)

    // 3. RIGHT JOIN
    println("---------- 3. RIGHT JOIN ----------")

    val rightJoin = employees
      .join(departments, joinCondition, "right")
      .select(
        employees("employee_id"),
        employees("employee_name"),
        departments("department_id"),
        departments("department_name"),
        departments("location"),
        employees("salary")
      )

    rightJoin.show(false)

    // 4. FULL OUTER JOIN
    println("---------- 4. FULL OUTER JOIN ----------")

    val fullOuterJoin = employees
      .join(departments, joinCondition, "full_outer")
      .select(
        employees("employee_id"),
        employees("employee_name"),
        departments("department_id"),
        departments("department_name"),
        departments("location"),
        employees("salary")
      )

    fullOuterJoin.show(false)

    // 5. LEFT SEMI JOIN
    println("---------- 5. LEFT SEMI JOIN ----------")

    val leftSemiJoin = employees
      .join(departments, joinCondition, "left_semi")

    leftSemiJoin.show(false)

    // 6. LEFT ANTI JOIN
    println("---------- 6. LEFT ANTI JOIN ----------")

    val leftAntiJoin = employees
      .join(departments, joinCondition, "left_anti")

    leftAntiJoin.show(false)

    // 7. INNER JOIN WITH FILTER
    println("---------- 7. JOIN + FILTER ----------")

    val highSalaryEmployees = employees
      .join(departments, joinCondition, "inner")
      .filter(col("salary") > 70000)
      .select(
        employees("employee_name"),
        departments("department_name"),
        employees("salary")
      )

    highSalaryEmployees.show(false)

    // 8. JOIN + AGGREGATION
    println("---------- 8. JOIN + AGGREGATION ----------")

    val departmentSalary = employees
      .join(departments, joinCondition, "inner")
      .groupBy(departments("department_name"))
      .agg(
        count(employees("employee_id")).alias("employee_count"),
        sum(employees("salary")).alias("total_salary"),
        avg(employees("salary")).alias("average_salary")
      )
      .orderBy(desc("total_salary"))

    departmentSalary.show(false)

    println("---------- JOIN CONCEPT ----------")

    println(
      """
        |Inner Join:
        |Returns only matching records from both datasets.
        |
        |Left Join:
        |Returns every record from the left dataset and
        |matching records from the right dataset.
        |
        |Right Join:
        |Returns every record from the right dataset and
        |matching records from the left dataset.
        |
        |Full Outer Join:
        |Returns all records from both datasets.
        |
        |Left Semi Join:
        |Returns records from the left dataset that have
        |a matching record in the right dataset.
        |
        |Left Anti Join:
        |Returns records from the left dataset that do not
        |have a matching record in the right dataset.
        |
        |Common real-world use cases:
        |- Combining employee and department information
        |- Joining customers with orders
        |- Joining products with sales
        |- Enriching transaction data
        |- Finding unmatched records
        |- Data quality validation
        |""".stripMargin
    )

    println("\n========== DAY 18 COMPLETED SUCCESSFULLY ==========\n")

    spark.stop()
  }
}
