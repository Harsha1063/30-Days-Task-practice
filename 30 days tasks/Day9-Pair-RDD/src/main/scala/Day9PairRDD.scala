import org.apache.spark.{SparkConf, SparkContext}

object Day9PairRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 9 - Pair RDD")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    println("==================================================")
    println("DAY 9 - PAIR RDD")
    println("==================================================")

    // --------------------------------------------------
    // 1. Create key-value RDD
    // --------------------------------------------------

    val transactions = sc.textFile("data/transactions.txt")

    val transactionRDD = transactions.map { line =>
      val parts = line.split(",")
      (
        parts(0),
        parts(1),
        parts(2).toDouble
      )
    }

    println("\nBank Transactions:")
    transactionRDD.collect().foreach(println)

    // --------------------------------------------------
    // 2. reduceByKey - Aggregate transactions by account
    // --------------------------------------------------

    println("\n==================================================")
    println("REDUCEBYKEY - TRANSACTIONS BY ACCOUNT")
    println("==================================================")

    val accountAmounts = transactionRDD.map {
      case (account, _, amount) =>
        (account, amount)
    }

    val accountTotals = accountAmounts.reduceByKey(_ + _)

    accountTotals.collect().sortBy(_._1).foreach {
      case (account, total) =>
        println(f"$account -> $total%.2f")
    }

    // --------------------------------------------------
    // 3. groupByKey - Group transaction amounts
    // --------------------------------------------------

    println("\n==================================================")
    println("GROUPBYKEY - TRANSACTIONS BY ACCOUNT")
    println("==================================================")

    val groupedAccounts = accountAmounts.groupByKey()

    groupedAccounts.collect().sortBy(_._1).foreach {
      case (account, amounts) =>
        println(s"$account -> ${amounts.mkString(", ")}")
    }

    // --------------------------------------------------
    // 4. mapValues - Transform values only
    // --------------------------------------------------

    println("\n==================================================")
    println("MAPVALUES - DOUBLE ACCOUNT TOTALS")
    println("==================================================")

    val doubledTotals = accountTotals.mapValues(_ * 2)

    doubledTotals.collect().sortBy(_._1).foreach {
      case (account, total) =>
        println(f"$account -> $total%.2f")
    }

    // --------------------------------------------------
    // 5. Product revenue
    // --------------------------------------------------

    println("\n==================================================")
    println("REVENUE BY PRODUCT")
    println("==================================================")

    val productSales = sc.parallelize(Seq(
      ("Laptop", 75000.0),
      ("Phone", 30000.0),
      ("Laptop", 50000.0),
      ("Tablet", 20000.0),
      ("Phone", 25000.0),
      ("Laptop", 45000.0),
      ("Tablet", 15000.0),
      ("Phone", 35000.0)
    ))

    val productRevenue = productSales.reduceByKey(_ + _)

    productRevenue.collect().sortBy(_._1).foreach {
      case (product, revenue) =>
        println(f"$product -> ₹$revenue%.2f")
    }

    // --------------------------------------------------
    // 6. Department revenue
    // --------------------------------------------------

    println("\n==================================================")
    println("REVENUE BY DEPARTMENT")
    println("==================================================")

    val departmentSales = sc.parallelize(Seq(
      ("Electronics", 75000.0),
      ("Mobile", 30000.0),
      ("Electronics", 50000.0),
      ("Home", 20000.0),
      ("Mobile", 25000.0),
      ("Electronics", 45000.0),
      ("Home", 15000.0),
      ("Mobile", 35000.0)
    ))

    val departmentRevenue = departmentSales.reduceByKey(_ + _)

    departmentRevenue.collect().sortBy(_._1).foreach {
      case (department, revenue) =>
        println(f"$department -> ₹$revenue%.2f")
    }

    // --------------------------------------------------
    // 7. reduceByKey vs groupByKey
    // --------------------------------------------------

    println("\n==================================================")
    println("REDUCEBYKEY VS GROUPBYKEY")
    println("==================================================")

    println("\nreduceByKey:")
    println("1. Performs local aggregation before shuffle.")
    println("2. Transfers less data across the network.")
    println("3. Usually more memory efficient.")
    println("4. Preferred for aggregation operations.")

    println("\ngroupByKey:")
    println("1. Groups all values belonging to each key.")
    println("2. More data may be transferred during shuffle.")
    println("3. Can consume more memory.")
    println("4. Useful when individual values are required.")

    println("\nPerformance comparison:")
    println("For simple aggregation such as sum, reduceByKey")
    println("is generally more efficient than groupByKey.")

    // --------------------------------------------------
    // 8. Bank transaction scenario
    // --------------------------------------------------

    println("\n==================================================")
    println("BANK TRANSACTION AGGREGATION")
    println("==================================================")

    val bankTransactions = sc.parallelize(Seq(
      ("ACC1001", 5000.0),
      ("ACC1002", -1500.0),
      ("ACC1001", -1000.0),
      ("ACC1003", 8000.0),
      ("ACC1002", 3000.0),
      ("ACC1001", 2500.0),
      ("ACC1003", -2000.0),
      ("ACC1002", -500.0),
      ("ACC1004", 6000.0),
      ("ACC1004", -1000.0)
    ))

    val accountBalances = bankTransactions.reduceByKey(_ + _)

    println("Net transaction amount by account:")

    accountBalances.collect().sortBy(_._1).foreach {
      case (account, balance) =>
        println(f"$account -> ₹$balance%.2f")
    }

    // --------------------------------------------------
    // 9. Pair RDD transformations summary
    // --------------------------------------------------

    println("\n==================================================")
    println("PAIR RDD SUMMARY")
    println("==================================================")

    println("Key-value RDD:")
    println("(key, value)")

    println("\nreduceByKey:")
    println("Combines values with the same key.")

    println("\ngroupByKey:")
    println("Groups all values with the same key.")

    println("\nmapValues:")
    println("Transforms only the values while preserving keys.")

    // --------------------------------------------------
    // 10. Actions
    // --------------------------------------------------

    println("\n==================================================")
    println("ACTIONS")
    println("==================================================")

    println(s"Number of accounts: ${accountTotals.count()}")

    println(s"Number of products: ${productRevenue.count()}")

    println(s"Number of departments: ${departmentRevenue.count()}")

    // --------------------------------------------------

    sc.stop()

    println("\n==================================================")
    println("PROGRAM COMPLETED")
    println("==================================================")
  }
}
