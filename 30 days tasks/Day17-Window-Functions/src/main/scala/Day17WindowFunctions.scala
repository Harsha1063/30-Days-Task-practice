import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17WindowFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day17 Window Functions")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n========== DAY 17: WINDOW FUNCTIONS ==========\n")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")

    println("---------- ORIGINAL DATA ----------")
    df.show(false)

    println("---------- 1. ROW_NUMBER ----------")

    val employeeWindow = Window
      .partitionBy("department")
      .orderBy(col("sales_amount").desc)

    val rowNumberDf = df.withColumn(
      "row_number",
      row_number().over(employeeWindow)
    )

    rowNumberDf.show(false)

    println("---------- 2. RANK ----------")

    val rankDf = df.withColumn(
      "rank",
      rank().over(employeeWindow)
    )

    rankDf.show(false)

    println("---------- 3. DENSE_RANK ----------")

    val denseRankDf = df.withColumn(
      "dense_rank",
      dense_rank().over(employeeWindow)
    )

    denseRankDf.show(false)

    println("---------- 4. LAG ----------")

    val employeeMonthWindow = Window
      .partitionBy("employee_id")
      .orderBy("month")

    val lagDf = df.withColumn(
      "previous_month_sales",
      lag("sales_amount", 1).over(employeeMonthWindow)
    )

    lagDf.show(false)

    println("---------- 5. LEAD ----------")

    val leadDf = df.withColumn(
      "next_month_sales",
      lead("sales_amount", 1).over(employeeMonthWindow)
    )

    leadDf.show(false)

    println("---------- 6. MONTHLY SALES CHANGE ----------")

    val changeDf = lagDf.withColumn(
      "sales_change",
      col("sales_amount") - col("previous_month_sales")
    )

    changeDf.show(false)

    println("---------- 7. RUNNING TOTAL ----------")

    val runningTotalWindow = Window
      .partitionBy("employee_id")
      .orderBy("month")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val runningTotalDf = df.withColumn(
      "running_total",
      sum("sales_amount").over(runningTotalWindow)
    )

    runningTotalDf.show(false)

    println("---------- 8. TOP PERFORMER IN EACH DEPARTMENT ----------")

    val topEmployees = rowNumberDf
      .filter(col("row_number") === 1)
      .select(
        "department",
        "employee_name",
        "sales_amount"
      )

    topEmployees.show(false)

    println("---------- 9. WINDOW FUNCTION CONCEPT ----------")

    println(
      """
        |Window functions perform calculations across related rows
        |without collapsing those rows into a single record.
        |
        |partitionBy:
        |Divides data into independent groups.
        |
        |orderBy:
        |Defines the order of rows inside each group.
        |
        |row_number:
        |Assigns a unique sequential number to each row.
        |
        |rank:
        |Assigns the same rank to tied values and leaves gaps.
        |
        |dense_rank:
        |Assigns the same rank to tied values without gaps.
        |
        |lag:
        |Accesses a previous row.
        |
        |lead:
        |Accesses a following row.
        |
        |Running total:
        |Calculates cumulative values across ordered rows.
        |
        |Common use cases:
        |- Top-N records
        |- Employee ranking
        |- Month-over-month comparison
        |- Running totals
        |- Previous/next event analysis
        |- Time-series analysis
        |""".stripMargin
    )

    println("\n========== DAY 17 COMPLETED SUCCESSFULLY ==========\n")

    spark.stop()
  }
}
