import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Day10Partitioning {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 10 - Partitioning")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    println("==================================================")
    println("DAY 10 - PARTITIONING")
    println("==================================================")

    // --------------------------------------------------
    // 1. Create RDD
    // --------------------------------------------------

    val sales = sc.textFile("data/sales.txt")

    println("\nInput RDD:")
    sales.collect().foreach(println)

    // --------------------------------------------------
    // 2. Inspect initial partition count
    // --------------------------------------------------

    println("\n==================================================")
    println("INITIAL PARTITION COUNT")
    println("==================================================")

    println(s"Initial partitions: ${sales.getNumPartitions}")

    println("\nPartition sizes:")

    sales.mapPartitionsWithIndex {
      case (partitionId, records) =>
        Iterator(
          s"Partition $partitionId -> ${records.size} records"
        )
    }.collect().foreach(println)

    // --------------------------------------------------
    // 3. Repartition
    // --------------------------------------------------

    println("\n==================================================")
    println("REPARTITION")
    println("==================================================")

    val repartitioned = sales.repartition(4)

    println(s"Partitions after repartition(4): ${repartitioned.getNumPartitions}")

    println("\nRepartition creates a shuffle.")
    println("It is useful when increasing partitions")
    println("or redistributing data more evenly.")

    // --------------------------------------------------
    // 4. Coalesce
    // --------------------------------------------------

    println("\n==================================================")
    println("COALESCE")
    println("==================================================")

    val coalesced = repartitioned.coalesce(2)

    println(s"Partitions after coalesce(2): ${coalesced.getNumPartitions}")

    println("\nCoalesce is commonly used to decrease partitions.")
    println("It usually avoids a full shuffle.")

    // --------------------------------------------------
    // 5. Compare partition counts
    // --------------------------------------------------

    println("\n==================================================")
    println("PARTITION COUNT COMPARISON")
    println("==================================================")

    println(s"Original:       ${sales.getNumPartitions}")
    println(s"Repartitioned:  ${repartitioned.getNumPartitions}")
    println(s"Coalesced:      ${coalesced.getNumPartitions}")

    // --------------------------------------------------
    // 6. When to increase/decrease partitions
    // --------------------------------------------------

    println("\n==================================================")
    println("WHEN TO INCREASE OR DECREASE PARTITIONS")
    println("==================================================")

    println("\nIncrease partitions when:")
    println("1. Dataset has too few partitions.")
    println("2. Available CPU cores are underutilized.")
    println("3. Individual partitions are too large.")
    println("4. More parallelism is required.")

    println("\nDecrease partitions when:")
    println("1. Dataset has too many small partitions.")
    println("2. Task scheduling overhead becomes high.")
    println("3. You want fewer output files.")
    println("4. The dataset is small after filtering.")

    // --------------------------------------------------
    // 7. Pair RDD
    // --------------------------------------------------

    println("\n==================================================")
    println("PAIR RDD")
    println("==================================================")

    val productSales = sales.map { line =>
      val parts = line.split(",")
      (parts(0), parts(2).toDouble)
    }

    println("Pair RDD:")
    productSales.collect().foreach(println)

    println(s"\nPair RDD partitions before partitionBy: ${productSales.getNumPartitions}")

    // --------------------------------------------------
    // 8. partitionBy
    // --------------------------------------------------

    println("\n==================================================")
    println("PARTITIONBY")
    println("==================================================")

    val partitionedPairRDD =
      productSales.partitionBy(new HashPartitioner(4))

    println(
      s"Pair RDD partitions after partitionBy(4): ${partitionedPairRDD.getNumPartitions}"
    )

    println("\npartitionBy explicitly assigns keys to partitions")
    println("using a partitioner.")

    println("\nPartitioner:")
    println(partitionedPairRDD.partitioner)

    // --------------------------------------------------
    // 9. Inspect Pair RDD partition distribution
    // --------------------------------------------------

    println("\n==================================================")
    println("PAIR RDD PARTITION DISTRIBUTION")
    println("==================================================")

    partitionedPairRDD.mapPartitionsWithIndex {
      case (partitionId, records) =>
        val data = records.toList
        Iterator(
          s"Partition $partitionId -> ${data.mkString(", ")}"
        )
    }.collect().foreach(println)

    // --------------------------------------------------
    // 10. Scenario - Too few partitions
    // --------------------------------------------------

    println("\n==================================================")
    println("SCENARIO - DATASET WITH TOO FEW PARTITIONS")
    println("==================================================")

    val smallPartitionRDD = sc.parallelize(
      1 to 20,
      1
    )

    println(
      s"Original partition count: ${smallPartitionRDD.getNumPartitions}"
    )

    println("Problem:")
    println("The dataset has only one partition.")
    println("Only one task can process the partition at a time.")
    println("Available CPU cores may remain underutilized.")

    val optimizedRDD = smallPartitionRDD.repartition(4)

    println(
      s"Optimized partition count: ${optimizedRDD.getNumPartitions}"
    )

    println("\nOptimization:")
    println("Increase partitions using repartition(4).")
    println("This creates more partitions and allows")
    println("more tasks to execute in parallel.")

    println("\nOptimized partition sizes:")

    optimizedRDD.mapPartitionsWithIndex {
      case (partitionId, records) =>
        Iterator(
          s"Partition $partitionId -> ${records.size} records"
        )
    }.collect().foreach(println)

    // --------------------------------------------------
    // 11. Repartition vs Coalesce
    // --------------------------------------------------

    println("\n==================================================")
    println("REPARTITION VS COALESCE")
    println("==================================================")

    println("\nrepartition:")
    println("1. Can increase or decrease partitions.")
    println("2. Performs a shuffle.")
    println("3. Redistributes data across partitions.")
    println("4. Useful for better load balancing.")

    println("\ncoalesce:")
    println("1. Primarily decreases partitions.")
    println("2. Usually avoids a full shuffle.")
    println("3. More efficient when reducing partitions.")
    println("4. Useful after filtering reduces dataset size.")

    // --------------------------------------------------
    // 12. Summary
    // --------------------------------------------------

    println("\n==================================================")
    println("PARTITIONING SUMMARY")
    println("==================================================")

    println("Partition = logical chunk of distributed data.")
    println("More partitions = potentially more parallelism.")
    println("Too many partitions = scheduling overhead.")
    println("Too few partitions = poor CPU utilization.")
    println("repartition = shuffle and redistribute.")
    println("coalesce = reduce partitions efficiently.")
    println("partitionBy = control Pair RDD partitioning.")

    // --------------------------------------------------

    sc.stop()

    println("\n==================================================")
    println("PROGRAM COMPLETED")
    println("==================================================")
  }
}
