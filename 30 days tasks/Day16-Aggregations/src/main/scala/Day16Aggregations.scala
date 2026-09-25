import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day16Aggregations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day16-Aggregations")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n========== DAY 16: AGGREGATIONS ==========")

    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")

    println("\n--- 1. ORIGINAL SALES DATA ---")
    sales.show()

    // Calculate total sales amount for every transaction
    val salesWithAmount = sales.withColumn(
      "sales_amount",
      col("quantity") * col("price")
    )

    println("\n--- 2. SALES AMOUNT ---")
    salesWithAmount.show()

    // ------------------------------------------------
    // 3. BASIC AGGREGATIONS
    // ------------------------------------------------

    println("\n--- 3. BASIC AGGREGATIONS ---")

    val basicStats = salesWithAmount.agg(
      count("*").alias("transaction_count"),
      sum("quantity").alias("total_quantity"),
      sum("sales_amount").alias("total_sales"),
      avg("sales_amount").alias("average_transaction"),
      min("sales_amount").alias("minimum_transaction"),
      max("sales_amount").alias("maximum_transaction")
    )

    basicStats.show()

    // ------------------------------------------------
    // 4. CATEGORY-WISE AGGREGATION
    // ------------------------------------------------

    println("\n--- 4. CATEGORY-WISE AGGREGATION ---")

    salesWithAmount
      .groupBy("category")
      .agg(
        count("*").alias("transactions"),
        sum("quantity").alias("total_quantity"),
        sum("sales_amount").alias("total_sales"),
        round(avg("sales_amount"), 2).alias("average_sales")
      )
      .orderBy(desc("total_sales"))
      .show()

    // ------------------------------------------------
    // 5. REGION-WISE AGGREGATION
    // ------------------------------------------------

    println("\n--- 5. REGION-WISE AGGREGATION ---")

    salesWithAmount
      .groupBy("region")
      .agg(
        count("*").alias("transactions"),
        sum("sales_amount").alias("total_sales"),
        round(avg("sales_amount"), 2).alias("average_sales")
      )
      .orderBy(desc("total_sales"))
      .show()

    // ------------------------------------------------
    // 6. MULTIPLE GROUPING COLUMNS
    // ------------------------------------------------

    println("\n--- 6. CATEGORY + REGION AGGREGATION ---")

    salesWithAmount
      .groupBy("category", "region")
      .agg(
        sum("quantity").alias("total_quantity"),
        sum("sales_amount").alias("total_sales")
      )
      .orderBy(desc("total_sales"))
      .show()

    // ------------------------------------------------
    // 7. HAVING-LIKE FILTER
    // ------------------------------------------------

    println("\n--- 7. CATEGORIES WITH SALES > 100000 ---")

    salesWithAmount
      .groupBy("category")
      .agg(
        sum("sales_amount").alias("total_sales")
      )
      .filter(col("total_sales") > 100000)
      .orderBy(desc("total_sales"))
      .show()

    // ------------------------------------------------
    // 8. TOP PRODUCTS BY SALES
    // ------------------------------------------------

    println("\n--- 8. TOP PRODUCTS BY SALES ---")

    salesWithAmount
      .groupBy("product")
      .agg(
        sum("quantity").alias("total_quantity"),
        sum("sales_amount").alias("total_sales")
      )
      .orderBy(desc("total_sales"))
      .show()

    // ------------------------------------------------
    // 9. AGGREGATION EXPLANATION
    // ------------------------------------------------

    println("\n--- 9. AGGREGATION EXPLANATION ---")

    println("count() counts records.")
    println("sum() calculates the total.")
    println("avg() calculates the average.")
    println("min() returns the minimum value.")
    println("max() returns the maximum value.")
    println("groupBy() creates groups before aggregation.")
    println("agg() allows multiple aggregation functions together.")

    println("\n========== DAY 16 COMPLETED SUCCESSFULLY ==========\n")

    spark.stop()
  }
}
