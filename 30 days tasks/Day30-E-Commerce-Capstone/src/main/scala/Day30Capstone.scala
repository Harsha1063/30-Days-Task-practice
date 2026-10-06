import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Day30Capstone {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day30-E-Commerce-Capstone")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputPath = args.headOption.getOrElse("data/orders.csv")
    val outputPath = args.drop(1).headOption.getOrElse("output")

    try {

      val schema = StructType(Seq(
        StructField("order_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("product_id", StringType, true),
        StructField("category", StringType, true),
        StructField("region", StringType, true),
        StructField("quantity", IntegerType, true),
        StructField("unit_price", DoubleType, true),
        StructField("order_status", StringType, true),
        StructField("order_date", StringType, true)
      ))

      // 1. Read raw CSV data.
      val rawOrders = spark.read
        .option("header", "true")
        .schema(schema)
        .csv(inputPath)

      println("\n========== RAW ORDERS ==========")
      rawOrders.show(20, false)

      println(s"Raw record count: ${rawOrders.count()}")

      // 2. Clean records and validate business fields.
      val cleanedOrders = rawOrders
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .filter(
          col("order_id").isNotNull &&
          length(trim(col("order_id"))) > 0 &&
          col("customer_id").isNotNull &&
          col("product_id").isNotNull &&
          col("category").isNotNull &&
          col("region").isNotNull &&
          col("quantity").isNotNull &&
          col("quantity") > 0 &&
          col("unit_price").isNotNull &&
          col("unit_price") >= 0 &&
          col("order_date").isNotNull &&
          col("order_status").isin("COMPLETED", "PENDING", "CANCELLED")
        )
        .dropDuplicates("order_id")

      println("\n========== DATA QUALITY ==========")
      println(s"Valid unique records: ${cleanedOrders.count()}")

      // 3. Calculate revenue for completed orders only.
      val completedOrders = cleanedOrders
        .filter(col("order_status") === "COMPLETED")
        .withColumn(
          "revenue",
          round(col("quantity") * col("unit_price"), 2)
        )
        .cache()

      println(s"Completed orders: ${completedOrders.count()}")

      completedOrders.createOrReplaceTempView("completed_orders")

      // 4. Overall business KPIs.
      println("\n========== OVERALL BUSINESS KPIs ==========")

      spark.sql("""
        SELECT
          COUNT(*) AS completed_orders,
          COUNT(DISTINCT customer_id) AS unique_customers,
          SUM(quantity) AS units_sold,
          ROUND(SUM(revenue), 2) AS total_revenue,
          ROUND(AVG(revenue), 2) AS average_order_value
        FROM completed_orders
      """).show(false)

      // 5. Product-level sales.
      println("\n========== PRODUCT SALES ==========")

      val productSales = completedOrders
        .groupBy("product_id", "category")
        .agg(
          count("*").as("order_count"),
          sum("quantity").as("units_sold"),
          round(sum("revenue"), 2).as("total_revenue")
        )
        .orderBy(desc("total_revenue"))

      productSales.show(false)

      // 6. Category-level performance using Spark SQL.
      println("\n========== CATEGORY PERFORMANCE ==========")

      spark.sql("""
        SELECT
          category,
          COUNT(*) AS order_count,
          SUM(quantity) AS units_sold,
          ROUND(SUM(revenue), 2) AS total_revenue
        FROM completed_orders
        GROUP BY category
        ORDER BY total_revenue DESC
      """).show(false)

      // 7. Regional performance.
      println("\n========== REGIONAL PERFORMANCE ==========")

      val regionalSales = completedOrders
        .groupBy("region")
        .agg(
          count("*").as("order_count"),
          round(sum("revenue"), 2).as("total_revenue")
        )
        .orderBy(desc("total_revenue"))

      regionalSales.show(false)

      // 8. Daily sales trend.
      println("\n========== DAILY SALES ==========")

      val dailySales = completedOrders
        .groupBy("order_date")
        .agg(
          count("*").as("order_count"),
          round(sum("revenue"), 2).as("daily_revenue")
        )
        .orderBy("order_date")

      dailySales.show(false)

      // 9. Persist analytics as Parquet.
      productSales.write
        .mode("overwrite")
        .parquet(s"$outputPath/product_sales")

      regionalSales.write
        .mode("overwrite")
        .partitionBy("region")
        .parquet(s"$outputPath/regional_sales")

      dailySales.write
        .mode("overwrite")
        .parquet(s"$outputPath/daily_sales")

      println(s"\nAnalytics successfully written to: $outputPath")
      println("Day 30 E-Commerce Capstone completed.")

      completedOrders.unpersist()

    } finally {
      spark.stop()
    }
  }
}
