import org.apache.spark.{SparkConf, SparkContext}

object Day8DAGSparkExecution {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 8 - DAG and Spark Execution")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    println("==================================================")
    println("DAY 8 - DAG AND SPARK EXECUTION")
    println("==================================================")

    // --------------------------------------------------
    // 1. Create the input RDD
    // --------------------------------------------------

    val sales = sc.textFile("data/sales.txt")

    println("\nInput Data:")
    sales.collect().foreach(println)

    // --------------------------------------------------
    // 2. Several transformations
    // --------------------------------------------------

    val parsed = sales.map { line =>
      val parts = line.split(",")
      (parts(0), parts(1).toInt)
    }

    // Narrow transformation
    val filtered = parsed.filter {
      case (_, amount) => amount >= 100
    }

    // Narrow transformation
    val doubled = filtered.map {
      case (product, amount) => (product, amount * 2)
    }

    // Wide transformation - shuffle boundary
    val totalSales = doubled.reduceByKey(_ + _)

    // Narrow transformation after reduceByKey
    val sortedSales = totalSales.sortBy {
      case (_, amount) => -amount
    }

    // --------------------------------------------------
    // 3. DAG Transformation Chain
    // --------------------------------------------------

    println("\n==================================================")
    println("DAG TRANSFORMATION CHAIN")
    println("==================================================")

    println("sales")
    println("  |")
    println("  | map")
    println("  v")
    println("parsed")
    println("  |")
    println("  | filter")
    println("  v")
    println("filtered")
    println("  |")
    println("  | map")
    println("  v")
    println("doubled")
    println("  |")
    println("  | reduceByKey")
    println("  |")
    println("  | ===== SHUFFLE BOUNDARY =====")
    println("  v")
    println("totalSales")
    println("  |")
    println("  | sortBy")
    println("  |")
    println("  | ===== SHUFFLE BOUNDARY =====")
    println("  v")
    println("sortedSales")

    // --------------------------------------------------
    // 4. Narrow vs Wide transformations
    // --------------------------------------------------

    println("\n==================================================")
    println("NARROW VS WIDE TRANSFORMATIONS")
    println("==================================================")

    println("\nNarrow Transformations:")
    println("1. map")
    println("2. filter")
    println("3. map after filter")
    println("4. Each output partition depends on one input partition.")
    println("5. No shuffle is required.")

    println("\nWide Transformations:")
    println("1. reduceByKey")
    println("2. sortBy")
    println("3. Data may need to move between partitions.")
    println("4. Shuffle is required.")
    println("5. Wide transformations create stage boundaries.")

    // --------------------------------------------------
    // 5. Job, Stage, Task and Partition
    // --------------------------------------------------

    println("\n==================================================")
    println("JOB, STAGE, TASK AND PARTITION")
    println("==================================================")

    println("\nJob:")
    println("A job is created when an action is executed.")

    println("\nStage:")
    println("A stage is a group of operations that can execute")
    println("without crossing a shuffle boundary.")

    println("\nTask:")
    println("A task is the unit of work that processes one")
    println("partition.")

    println("\nPartition:")
    println("A partition is a logical chunk of an RDD.")

    // --------------------------------------------------
    // 6. Predict stages for reduceByKey pipeline
    // --------------------------------------------------

    println("\n==================================================")
    println("STAGE PREDICTION - REDUCEBYKEY")
    println("==================================================")

    println("Pipeline:")
    println("textFile -> map -> filter -> map -> reduceByKey")

    println("\nPredicted stages:")
    println("Stage 0:")
    println("textFile -> map -> filter -> map")

    println("Stage 1:")
    println("reduceByKey")

    println("\nReason:")
    println("map and filter are narrow transformations.")
    println("reduceByKey is a wide transformation.")
    println("The shuffle created by reduceByKey separates")
    println("the pipeline into two stages.")

    // --------------------------------------------------
    // 7. First Action
    // --------------------------------------------------

    println("\n==================================================")
    println("ACTION 1 - COLLECT")
    println("==================================================")

    val result = totalSales.collect()

    println("Total sales after reduceByKey:")
    result.foreach {
      case (product, amount) =>
        println(s"$product -> $amount")
    }

    // --------------------------------------------------
    // 8. Second Action
    // --------------------------------------------------

    println("\n==================================================")
    println("ACTION 2 - COUNT")
    println("==================================================")

    println(s"Number of products: ${totalSales.count()}")

    // --------------------------------------------------
    // 9. Third Action
    // --------------------------------------------------

    println("\n==================================================")
    println("ACTION 3 - SORTED RESULT")
    println("==================================================")

    sortedSales.collect().foreach {
      case (product, amount) =>
        println(s"$product -> $amount")
    }

    // --------------------------------------------------
    // 10. Number of partitions
    // --------------------------------------------------

    println("\n==================================================")
    println("PARTITION INFORMATION")
    println("==================================================")

    println(s"Input partitions: ${sales.getNumPartitions}")
    println(s"Parsed partitions: ${parsed.getNumPartitions}")
    println(s"Filtered partitions: ${filtered.getNumPartitions}")
    println(s"Doubled partitions: ${doubled.getNumPartitions}")
    println(s"Reduced partitions: ${totalSales.getNumPartitions}")

    // --------------------------------------------------
    // 11. Actual RDD Lineage
    // --------------------------------------------------

    println("\n==================================================")
    println("RDD LINEAGE / DAG DEBUG STRING")
    println("==================================================")

    println(totalSales.toDebugString)

    // --------------------------------------------------
    // 12. Execution explanation
    // --------------------------------------------------

    println("\n==================================================")
    println("SPARK EXECUTION SUMMARY")
    println("==================================================")

    println("1. The driver creates the Spark job.")
    println("2. Spark builds a DAG from the transformations.")
    println("3. The DAG is divided into stages at shuffle boundaries.")
    println("4. Each stage is divided into tasks.")
    println("5. Each task processes one partition.")
    println("6. reduceByKey causes a shuffle.")
    println("7. The action triggers actual execution.")

    sc.stop()

    println("\n==================================================")
    println("PROGRAM COMPLETED")
    println("==================================================")
  }
}
