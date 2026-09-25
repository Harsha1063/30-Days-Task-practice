import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day20FileFormatsOutput {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20 File Formats and Output")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    println("\n========== DAY 20: FILE FORMATS & OUTPUT ==========\n")

    // ---------------------------------------------------------
    // 1. READ CSV
    // ---------------------------------------------------------

    println("---------- 1. READ CSV ----------")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    products.show(false)

    products.printSchema()

    // ---------------------------------------------------------
    // 2. CREATE DERIVED COLUMN
    // ---------------------------------------------------------

    println("---------- 2. CALCULATED SALES VALUE ----------")

    val salesDf = products.withColumn(
      "total_value",
      col("price") * col("quantity")
    )

    salesDf.show(false)

    // ---------------------------------------------------------
    // 3. WRITE CSV
    // ---------------------------------------------------------

    println("---------- 3. WRITE CSV ----------")

    salesDf.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/csv")

    println("CSV output written successfully.")

    // ---------------------------------------------------------
    // 4. WRITE JSON
    // ---------------------------------------------------------

    println("---------- 4. WRITE JSON ----------")

    salesDf.write
      .mode("overwrite")
      .json("output/json")

    println("JSON output written successfully.")

    // ---------------------------------------------------------
    // 5. WRITE PARQUET
    // ---------------------------------------------------------

    println("---------- 5. WRITE PARQUET ----------")

    salesDf.write
      .mode("overwrite")
      .parquet("output/parquet")

    println("Parquet output written successfully.")

    // ---------------------------------------------------------
    // 6. PARTITIONED PARQUET
    // ---------------------------------------------------------

    println("---------- 6. PARTITIONED PARQUET ----------")

    salesDf.write
      .mode("overwrite")
      .partitionBy("category")
      .parquet("output/parquet_partitioned")

    println("Partitioned Parquet output written successfully.")

    // ---------------------------------------------------------
    // 7. APPEND MODE
    // ---------------------------------------------------------

    println("---------- 7. APPEND MODE ----------")

    val additionalProduct = Seq(
      ("P009", "Tablet", "Electronics", 30000, 8, 240000)
    ).toDF(
      "product_id",
      "product_name",
      "category",
      "price",
      "quantity",
      "total_value"
    )

    additionalProduct.write
      .mode("append")
      .parquet("output/parquet")

    println("Additional record appended successfully.")

    // ---------------------------------------------------------
    // 8. READ PARQUET BACK
    // ---------------------------------------------------------

    println("---------- 8. READ PARQUET ----------")

    val parquetDf = spark.read
      .parquet("output/parquet")

    parquetDf.show(false)

    // ---------------------------------------------------------
    // 9. READ JSON BACK
    // ---------------------------------------------------------

    println("---------- 9. READ JSON ----------")

    val jsonDf = spark.read
      .json("output/json")

    jsonDf.show(false)

    // ---------------------------------------------------------
    // 10. READ PARTITIONED PARQUET
    // ---------------------------------------------------------

    println("---------- 10. READ PARTITIONED PARQUET ----------")

    val partitionedDf = spark.read
      .parquet("output/parquet_partitioned")

    partitionedDf.show(false)

    // ---------------------------------------------------------
    // 11. FILE FORMAT COMPARISON
    // ---------------------------------------------------------

    println("---------- FILE FORMAT COMPARISON ----------")

    println(
      """
        |CSV:
        |- Human-readable
        |- Easy to exchange
        |- Schema is not stored with the data
        |- Usually larger than columnar formats
        |
        |JSON:
        |- Human-readable
        |- Supports nested structures
        |- Useful for APIs and semi-structured data
        |
        |Parquet:
        |- Columnar storage format
        |- Stores schema
        |- Efficient for analytical workloads
        |- Supports compression
        |- Commonly used with Spark
        |
        |Overwrite:
        |Replaces the existing output.
        |
        |Append:
        |Adds new data to existing output.
        |
        |Partitioning:
        |Organizes output into directory partitions based
        |on one or more columns.
        |
        |Example:
        |output/parquet_partitioned/category=Electronics/
        |output/parquet_partitioned/category=Clothing/
        |output/parquet_partitioned/category=Grocery/
        |""".stripMargin
    )

    println("\n========== DAY 20 COMPLETED SUCCESSFULLY ==========\n")

    spark.stop()
  }
}
