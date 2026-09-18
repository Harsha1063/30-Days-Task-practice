import org.apache.spark.{SparkConf, SparkContext}

object Day11BroadcastAccumulators {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 11 - Broadcast and Accumulators")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("==============================================")
    println(" DAY 11 - BROADCAST AND ACCUMULATORS")
    println("==============================================")

    // ------------------------------------------------
    // 1. Small product master/reference table
    // ------------------------------------------------

    val productMaster = Map(
      "P001" -> ("Laptop", 75000),
      "P002" -> ("Phone", 30000),
      "P003" -> ("Tablet", 20000),
      "P004" -> ("Monitor", 18000),
      "P005" -> ("Keyboard", 5000),
      "P006" -> ("Mouse", 2500)
    )

    println("\n===== Product Master Table =====")

    productMaster.toSeq.sortBy(_._1).foreach {
      case (productId, (productName, price)) =>
        println(s"$productId -> $productName -> Rs.$price")
    }

    // ------------------------------------------------
    // 2. Broadcast the small reference map
    // ------------------------------------------------

    val broadcastProducts = sc.broadcast(productMaster)

    println("\n===== Broadcast Variable =====")
    println("Product master map has been broadcast to executors.")
    println(s"Broadcast entries: ${broadcastProducts.value.size}")

    // ------------------------------------------------
    // 3. Read transaction data
    // ------------------------------------------------

    val transactions = sc.textFile("data/transactions.txt")

    println("\n===== Transactions =====")

    transactions.collect().foreach(println)

    println(s"\nInput partitions: ${transactions.getNumPartitions}")

    // ------------------------------------------------
    // 4. Accumulator for bad records
    // ------------------------------------------------

    val badRecords = sc.longAccumulator("Bad Records")

    // ------------------------------------------------
    // 5. Validate transactions using broadcast data
    // ------------------------------------------------

    val validatedTransactions = transactions.map { line =>

      val parts = line.split(",")

      if (parts.length != 3) {

        badRecords.add(1)

        s"INVALID FORMAT -> $line"

      } else {

        val transactionId = parts(0)
        val productId = parts(1)
        val quantity = parts(2).toInt

        val productMap = broadcastProducts.value

        productMap.get(productId) match {

          case Some((productName, price)) =>

            val total = price * quantity

            s"VALID -> $transactionId | $productId | $productName | Qty=$quantity | Total=Rs.$total"

          case None =>

            badRecords.add(1)

            s"INVALID PRODUCT -> $transactionId | $productId | Qty=$quantity"
        }
      }
    }

    // ------------------------------------------------
    // 6. Execute validation
    // ------------------------------------------------

    println("\n===== Validated Transactions =====")

    validatedTransactions.collect().foreach(println)

    // ------------------------------------------------
    // 7. Display accumulator result
    // ------------------------------------------------

    println("\n===== Accumulator Result =====")
    println(s"Bad records: ${badRecords.value}")

    // ------------------------------------------------
    // 8. Explain driver variable problem
    // ------------------------------------------------

    println("\n===== Driver Variable vs Accumulator =====")

    println(
      """
A normal driver variable should not be used for distributed updates.

Executors process RDD partitions independently.
A normal variable on the driver is not designed for reliable
updates from executor tasks.

An accumulator is designed for supported distributed aggregation
such as counters and sums. Executors update the accumulator and
the driver can read its value after an action.
"""
    )

    // ------------------------------------------------
    // 9. Scenario explanation
    // ------------------------------------------------

    println("\n===== Scenario: Transaction Validation =====")

    println(
      """
Scenario:
The transaction dataset is large, while the product master table
is small.

Instead of sending the product master data repeatedly with every
task, Spark broadcasts the small reference map to the executors.

Each executor can then validate its transaction records locally
using the broadcast data.

The accumulator counts invalid transactions across the distributed
processing tasks.
"""
    )

    // ------------------------------------------------
    // 10. Summary
    // ------------------------------------------------

    println("\n===== Day 11 Summary =====")

    println("1. Broadcast variables efficiently share small read-only data.")
    println("2. The product master map was broadcast to executors.")
    println("3. Transactions were validated using the broadcast map.")
    println("4. An accumulator counted bad transaction records.")
    println("5. Normal driver variables should not be used for executor updates.")
    println("6. Broadcast + accumulator is useful for distributed validation.")

    // Clean up
    broadcastProducts.destroy()

    sc.stop()

    println("\n==============================================")
    println(" DAY 11 COMPLETED SUCCESSFULLY")
    println("==============================================")
  }
}
