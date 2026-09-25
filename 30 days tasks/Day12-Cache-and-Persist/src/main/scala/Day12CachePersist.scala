import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import scala.util.Try

object Day12CachePersist {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day12-Cache-and-Persist")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    println("\n========== DAY 12: CACHE AND PERSIST ==========")

    val rawTransactions = sc.textFile("data/transactions.txt")

    val cleanedTransactions = rawTransactions
      .map(_.split(","))
      .filter(parts =>
        parts.length == 5 &&
        Try(parts(3).toDouble).isSuccess &&
        parts(4) == "VALID"
      )
      .map(parts => (parts(0), parts(1), parts(2), parts(3).toDouble))

    println(s"\nRaw transaction records: ${rawTransactions.count()}")

    println("\n--- 1. CACHE() ---")

    cleanedTransactions.cache()

    println(s"Storage level after cache(): ${cleanedTransactions.getStorageLevel}")

    println("\nReport 1 - Number of valid transactions")
    val validCount = cleanedTransactions.count()
    println(s"Valid transactions: $validCount")

    println("\nReport 2 - Total transaction value")
    val totalValue = cleanedTransactions
      .map(_._4)
      .sum()

    println(f"Total value: $totalValue%.2f")

    println("\nReport 3 - Transactions by category")
    cleanedTransactions
      .map(x => (x._3, 1))
      .reduceByKey(_ + _)
      .collect()
      .sortBy(_._1)
      .foreach {
        case (category, count) =>
          println(s"$category -> $count")
      }

    println("\n--- 2. CACHE() EXPLANATION ---")
    println("cache() stores the RDD using Spark's default storage level.")
    println("The same cleaned RDD can then be reused by multiple actions.")
    println("This avoids recomputing the cleaning transformations repeatedly.")

    cleanedTransactions.unpersist()

    println("\n--- 3. PERSIST(MEMORY_ONLY) ---")

    cleanedTransactions.persist(StorageLevel.MEMORY_ONLY)

    println(s"Storage level: ${cleanedTransactions.getStorageLevel}")
    println(s"Transaction count: ${cleanedTransactions.count()}")

    cleanedTransactions.unpersist()

    println("\n--- 4. PERSIST(MEMORY_AND_DISK) ---")

    cleanedTransactions.persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Storage level: ${cleanedTransactions.getStorageLevel}")
    println(
      f"Average transaction value: ${cleanedTransactions.map(_._4).mean()}%.2f"
    )

    cleanedTransactions.unpersist()

    println("\n--- 5. WHEN CACHING CAN HURT PERFORMANCE ---")

    println("Caching can hurt performance when:")
    println("1. The RDD is used only once.")
    println("2. The dataset is too large for available memory.")
    println("3. Caching causes memory pressure or eviction.")
    println("4. The cost of caching is greater than recomputing the data.")

    println("\n========== DAY 12 COMPLETED SUCCESSFULLY ==========\n")

    sc.stop()
  }
}
