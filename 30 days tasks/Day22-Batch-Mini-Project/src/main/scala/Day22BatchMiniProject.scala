import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Day22BatchMiniProject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day22-Batch-Mini-Project")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    println("\n" + "=" * 70)
    println("DAY 22 - BATCH MINI PROJECT")
    println("=" * 70)

    // ------------------------------------------------------------
    // 1. READ RAW DATA
    // ------------------------------------------------------------

    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    println("\n--- RAW TRANSACTIONS ---")
    transactions.show(false)

    println("\nRaw transaction count: " + transactions.count())

    // ------------------------------------------------------------
    // 2. CLEAN INVALID RECORDS
    // ------------------------------------------------------------

    val validTransactions = transactions
      .filter(col("transaction_id").isNotNull)
      .filter(col("customer_id").isNotNull)
      .filter(col("product_id").isNotNull)
      .filter(col("quantity") > 0)
      .filter(col("status") === "COMPLETED")

    println("\n--- CLEANED TRANSACTIONS ---")
    validTransactions.show(false)

    println("Valid transaction count: " + validTransactions.count())

    println(
      "Invalid/removed records: " +
      (transactions.count() - validTransactions.count())
    )

    // ------------------------------------------------------------
    // 3. JOIN CUSTOMER DATA
    // ------------------------------------------------------------

    val customerJoined = validTransactions
      .join(
        customers,
        validTransactions("customer_id") === customers("customer_id"),
        "inner"
      )
      .drop(customers("customer_id"))

    println("\n--- AFTER CUSTOMER JOIN ---")
    customerJoined.show(false)

    // ------------------------------------------------------------
    // 4. JOIN PRODUCT DATA
    // ------------------------------------------------------------

    val enriched = customerJoined
      .join(
        products,
        customerJoined("product_id") === products("product_id"),
        "inner"
      )
      .drop(products("product_id"))

    println("\n--- ENRICHED TRANSACTIONS ---")
    enriched.show(false)

    // ------------------------------------------------------------
    // 5. CALCULATE REVENUE
    // ------------------------------------------------------------

    val sales = enriched
      .withColumn(
        "revenue",
        col("quantity") * col("price")
      )

    println("\n--- SALES WITH REVENUE ---")
    sales.select(
      "transaction_id",
      "customer_name",
      "city",
      "product_name",
      "category",
      "quantity",
      "price",
      "revenue",
      "transaction_date"
    ).show(false)

    // ------------------------------------------------------------
    // 6. CACHE BECAUSE SALES IS USED MULTIPLE TIMES
    // ------------------------------------------------------------

    sales.cache()

    println("\nCached sales count: " + sales.count())

    // ------------------------------------------------------------
    // 7. DAILY SALES AGGREGATION
    // ------------------------------------------------------------

    val dailySales = sales
      .groupBy("transaction_date")
      .agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("units_sold"),
        countDistinct("customer_id").alias("unique_customers")
      )
      .orderBy("transaction_date")

    println("\n--- DAILY SALES ---")
    dailySales.show(false)

    // ------------------------------------------------------------
    // 8. CATEGORY SALES
    // ------------------------------------------------------------

    val categorySales = sales
      .groupBy("transaction_date", "category")
      .agg(
        sum("revenue").alias("category_revenue"),
        sum("quantity").alias("units_sold")
      )
      .orderBy(col("transaction_date"), desc("category_revenue"))

    println("\n--- CATEGORY SALES ---")
    categorySales.show(false)

    // ------------------------------------------------------------
    // 9. CITY SALES
    // ------------------------------------------------------------

    val citySales = sales
      .groupBy("city")
      .agg(
        sum("revenue").alias("city_revenue"),
        count("*").alias("transactions")
      )
      .orderBy(desc("city_revenue"))

    println("\n--- CITY SALES ---")
    citySales.show(false)

    // ------------------------------------------------------------
    // 10. TOP PRODUCTS
    // ------------------------------------------------------------

    val topProducts = sales
      .groupBy("product_id", "product_name")
      .agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("units_sold")
      )
      .orderBy(desc("total_revenue"))

    println("\n--- TOP PRODUCTS ---")
    topProducts.show(false)

    // ------------------------------------------------------------
    // 11. WRITE PARTITIONED PARQUET
    // ------------------------------------------------------------

    val outputPath = "output/daily-sales"

    dailySales
      .repartition(col("transaction_date"))
      .write
      .mode("overwrite")
      .partitionBy("transaction_date")
      .parquet(outputPath)

    println("\nPartitioned Parquet written to: " + outputPath)

    // ------------------------------------------------------------
    // 12. READ OUTPUT BACK
    // ------------------------------------------------------------

    val outputData = spark.read.parquet(outputPath)

    println("\n--- OUTPUT PARQUET ---")
    outputData.show(false)

    // ------------------------------------------------------------
    // 13. EXPLAIN EXECUTION
    // ------------------------------------------------------------

    println("\n--- EXECUTION PLAN ---")
    dailySales.explain(true)

    // ------------------------------------------------------------
    // 14. CONCEPT SUMMARY
    // ------------------------------------------------------------

    println("\n" + "=" * 70)
    println("BATCH PIPELINE")
    println("=" * 70)

    println(
      """
      Raw Transactions
             |
             v
      Data Cleaning
             |
             v
      Customer Join
             |
             v
      Product Join
             |
             v
      Revenue Calculation
             |
             v
      Aggregations
             |
             v
      Partitioned Parquet
      """.stripMargin
    )

    println("Cleaning      : Removed null IDs, invalid quantity and cancelled orders")
    println("Enrichment    : Joined transaction + customer + product data")
    println("Transformation: Calculated revenue = quantity * price")
    println("Aggregation   : Daily, category, city and product sales")
    println("Storage       : Partitioned Parquet")
    println("Optimization  : Cache + repartition before partitioned write")
    println("Shuffle        : GroupBy and joins may cause shuffle")
    println("Action        : count(), show(), write()")
    println("Lazy execution: Transformations execute when an action is called")

    sales.unpersist()

    println("\n" + "=" * 70)
    println("DAY 22 COMPLETED SUCCESSFULLY")
    println("=" * 70)

    spark.stop()
  }
}
