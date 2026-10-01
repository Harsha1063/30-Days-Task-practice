import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day21SparkCatalog {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day21-Spark-Catalog")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n" + "=" * 70)
    println("DAY 21 - SPARK CATALOG")
    println("=" * 70)

    // ------------------------------------------------------------
    // 1. LIST DEFAULT DATABASES
    // ------------------------------------------------------------

    println("\n--- DATABASES BEFORE CREATION ---")
    spark.catalog.listDatabases().show(false)

    // ------------------------------------------------------------
    // 2. READ HOTEL BOOKING DATA
    // ------------------------------------------------------------

    val bookings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hotel_bookings.csv")

    println("\n--- HOTEL BOOKINGS ---")
    bookings.show(false)

    println("\n--- BOOKING SCHEMA ---")
    bookings.printSchema()

    // ------------------------------------------------------------
    // 3. CREATE TEMPORARY VIEW
    // ------------------------------------------------------------

    bookings.createOrReplaceTempView("hotel_bookings")

    println("\n--- CATALOG TABLES AFTER TEMP VIEW ---")
    spark.catalog.listTables().show(false)

    // ------------------------------------------------------------
    // 4. QUERY TEMP VIEW
    // ------------------------------------------------------------

    println("\n--- CONFIRMED BOOKINGS ---")

    spark.sql(
      """
        |SELECT booking_id, customer_name, hotel, city, room_type, price
        |FROM hotel_bookings
        |WHERE status = 'Confirmed'
        |ORDER BY price DESC
        |""".stripMargin
    ).show(false)

    // ------------------------------------------------------------
    // 5. HOTEL REVENUE
    // ------------------------------------------------------------

    println("\n--- HOTEL REVENUE ---")

    spark.sql(
      """
        |SELECT
        |  hotel,
        |  COUNT(*) AS confirmed_bookings,
        |  SUM(price) AS revenue
        |FROM hotel_bookings
        |WHERE status = 'Confirmed'
        |GROUP BY hotel
        |ORDER BY revenue DESC
        |""".stripMargin
    ).show(false)

    // ------------------------------------------------------------
    // 6. CREATE DATABASE
    // ------------------------------------------------------------

    spark.sql("CREATE DATABASE IF NOT EXISTS hotel_analytics")

    println("\n--- DATABASES AFTER CREATION ---")
    spark.catalog.listDatabases().show(false)

    // ------------------------------------------------------------
    // 7. CREATE PERMANENT TABLE
    // ------------------------------------------------------------

    spark.sql("DROP TABLE IF EXISTS hotel_analytics.bookings")

    bookings.write
      .mode("overwrite")
      .saveAsTable("hotel_analytics.bookings")

    println("\n--- TABLES IN hotel_analytics ---")

    spark.catalog
      .listTables("hotel_analytics")
      .show(false)

    // ------------------------------------------------------------
    // 8. QUERY PERMANENT TABLE
    // ------------------------------------------------------------

    println("\n--- HOTEL ANALYTICS TABLE ---")

    spark.sql(
      """
        |SELECT
        |  city,
        |  COUNT(*) AS bookings,
        |  SUM(CASE WHEN status = 'Confirmed' THEN price ELSE 0 END) AS revenue
        |FROM hotel_analytics.bookings
        |GROUP BY city
        |ORDER BY revenue DESC
        |""".stripMargin
    ).show(false)

    // ------------------------------------------------------------
    // 9. TABLE METADATA
    // ------------------------------------------------------------

    println("\n--- TABLE COLUMNS / METADATA ---")

    spark.catalog
      .listColumns("hotel_analytics.bookings")
      .show(false)

    // ------------------------------------------------------------
    // 10. TABLE EXISTENCE CHECK
    // ------------------------------------------------------------

    println("\n--- TABLE EXISTENCE ---")

    println(
      "hotel_analytics.bookings exists: " +
        spark.catalog.tableExists("hotel_analytics.bookings")
    )

    println(
      "hotel_bookings temporary view exists: " +
        spark.catalog.tableExists("hotel_bookings")
    )

    // ------------------------------------------------------------
    // 11. CACHE TABLE
    // ------------------------------------------------------------

    println("\n--- CACHE TABLE ---")

    spark.catalog.cacheTable("hotel_analytics.bookings")

    println(
      "Table cached: " +
        spark.catalog.isCached("hotel_analytics.bookings")
    )

    spark.sql(
      """
        |SELECT hotel, COUNT(*) AS bookings
        |FROM hotel_analytics.bookings
        |GROUP BY hotel
        |""".stripMargin
    ).show(false)

    spark.catalog.uncacheTable("hotel_analytics.bookings")

    println(
      "Table cached after uncache: " +
        spark.catalog.isCached("hotel_analytics.bookings")
    )

    // ------------------------------------------------------------
    // 12. CATALOG CONCEPTS
    // ------------------------------------------------------------

    println("\n" + "=" * 70)
    println("SPARK CATALOG CONCEPTS")
    println("=" * 70)

    println(
      """
      Spark Catalog
          |
          +-- Databases
          |     +-- default
          |     +-- hotel_analytics
          |
          +-- Tables
          |     +-- hotel_analytics.bookings
          |
          +-- Temporary Views
          |     +-- hotel_bookings
          |
          +-- Metadata
                +-- Columns
                +-- Schema
                +-- Table existence
                +-- Cache status
      """.stripMargin
    )

    println("Database      : Logical namespace for tables")
    println("Temporary View: Session-level SQL view")
    println("Table         : Persistent Spark SQL table")
    println("Catalog       : Metadata management interface")
    println("Cache         : Keeps table data available for repeated queries")

    println("\n" + "=" * 70)
    println("DAY 21 COMPLETED SUCCESSFULLY")
    println("=" * 70)

    spark.stop()
  }
}
