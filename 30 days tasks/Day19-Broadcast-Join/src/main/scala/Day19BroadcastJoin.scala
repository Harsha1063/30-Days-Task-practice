import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day19BroadcastJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19 Broadcast Join")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n========== DAY 19: BROADCAST JOIN ==========\n")

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

    println("---------- DATASET SIZES ----------")

    println(s"Employee records: ${employees.count()}")
    println(s"Department records: ${departments.count()}")

    // ---------------------------------------------------------
    // 1. NORMAL INNER JOIN
    // ---------------------------------------------------------

    println("\n---------- 1. NORMAL INNER JOIN ----------")

    val normalJoin = employees
      .join(
        departments,
        employees("department_id") === departments("department_id"),
        "inner"
      )
      .select(
        employees("employee_id"),
        employees("employee_name"),
        departments("department_name"),
        departments("location"),
        employees("salary")
      )

    normalJoin.show(false)

    println("---------- NORMAL JOIN EXECUTION PLAN ----------")

    normalJoin.explain()

    // ---------------------------------------------------------
    // 2. BROADCAST JOIN
    // ---------------------------------------------------------

    println("\n---------- 2. BROADCAST JOIN ----------")

    val broadcastJoin = employees
      .join(
        broadcast(departments),
        employees("department_id") === departments("department_id"),
        "inner"
      )
      .select(
        employees("employee_id"),
        employees("employee_name"),
        departments("department_name"),
        departments("location"),
        employees("salary")
      )

    broadcastJoin.show(false)

    println("---------- BROADCAST JOIN EXECUTION PLAN ----------")

    broadcastJoin.explain()

    // ---------------------------------------------------------
    // 3. BROADCAST JOIN + FILTER
    // ---------------------------------------------------------

    println("\n---------- 3. BROADCAST JOIN + FILTER ----------")

    val highSalaryEmployees = employees
      .join(
        broadcast(departments),
        employees("department_id") === departments("department_id"),
        "inner"
      )
      .filter(employees("salary") > 70000)
      .select(
        employees("employee_id"),
        employees("employee_name"),
        departments("department_name"),
        employees("salary")
      )

    highSalaryEmployees.show(false)

    // ---------------------------------------------------------
    // 4. BROADCAST JOIN + AGGREGATION
    // ---------------------------------------------------------

    println("\n---------- 4. BROADCAST JOIN + AGGREGATION ----------")

    val departmentSummary = employees
      .join(
        broadcast(departments),
        employees("department_id") === departments("department_id"),
        "inner"
      )
      .groupBy(departments("department_name"))
      .agg(
        count(employees("employee_id")).alias("employee_count"),
        sum(employees("salary")).alias("total_salary"),
        round(avg(employees("salary")), 2).alias("average_salary")
      )
      .orderBy(desc("total_salary"))

    departmentSummary.show(false)

    // ---------------------------------------------------------
    // 5. BROADCAST JOIN WITH SELECTED COLUMNS
    // ---------------------------------------------------------

    println("\n---------- 5. SELECTED COLUMNS ----------")

    val optimizedJoin = employees
      .select(
        "employee_id",
        "employee_name",
        "department_id",
        "salary"
      )
      .join(
        broadcast(
          departments.select(
            "department_id",
            "department_name"
          )
        ),
        Seq("department_id"),
        "inner"
      )

    optimizedJoin.show(false)

    // ---------------------------------------------------------
    // 6. EXPLANATION
    // ---------------------------------------------------------

    println("\n---------- BROADCAST JOIN CONCEPT ----------")

    println(
      """
        |Broadcast Join:
        |
        |A broadcast join sends a small DataFrame to every
        |executor so that Spark can perform the join locally
        |without shuffling the large DataFrame.
        |
        |Normal Join:
        |The data may need to be shuffled across executors
        |based on the join key.
        |
        |broadcast():
        |Explicitly tells Spark that the specified DataFrame
        |should be broadcast to the executors.
        |
        |Best suited for:
        |- Large fact table + small lookup table
        |- Employee data + department lookup
        |- Transactions + product master
        |- Orders + customer lookup
        |- Events + small reference data
        |
        |Advantages:
        |- Reduces shuffle
        |- Can improve join performance
        |- Useful for small dimension tables
        |
        |Important limitation:
        |Do NOT broadcast a very large DataFrame because
        |the broadcast data must fit in executor memory.
        |
        |The actual decision should consider:
        |- Size of the smaller dataset
        |- Executor memory
        |- Number of executors
        |- Network overhead
        |- Spark's auto broadcast threshold
        |""".stripMargin
    )

    println("\n========== DAY 19 COMPLETED SUCCESSFULLY ==========\n")

    spark.stop()
  }
}
