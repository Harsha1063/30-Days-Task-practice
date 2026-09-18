import org.apache.spark.{SparkConf, SparkContext}

object Day7ImmutabilityLineageFaultTolerance {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 7 - Immutability, Lineage and Fault Tolerance")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    println("==================================================")
    println("DAY 7 - IMMUTABILITY, LINEAGE AND FAULT TOLERANCE")
    println("==================================================")

    // 1. Original RDD

    val numbers = sc.textFile("data/numbers.txt")

    println("\nOriginal RDD:")
    println(numbers.collect().mkString(", "))

    // 2. Multi-step transformation chain

    val doubled = numbers.map(x => x.toInt * 2)

    val evenNumbers = doubled.filter(x => x % 2 == 0)

    val squared = evenNumbers.map(x => x * x)

    val finalRDD = squared.map(x => x + 10)

    println("\nTransformation Chain:")
    println("numbers -> map -> doubled")
    println("doubled -> filter -> evenNumbers")
    println("evenNumbers -> map -> squared")
    println("squared -> map -> finalRDD")

    // 3. RDD Immutability

    println("\nRDD Immutability:")

    println("Original RDD:")
    println(numbers.collect().mkString(", "))

    println("Transformed RDD:")
    println(doubled.collect().mkString(", "))

    println("The original RDD remains unchanged.")
    println("Spark creates a new RDD for each transformation.")

    // 4. Complete transformation chain

    println("\nFinal RDD Result:")
    println(finalRDD.collect().mkString(", "))

    // 5. RDD Lineage

    println("\n==================================================")
    println("RDD LINEAGE")
    println("==================================================")

    println(finalRDD.toDebugString)

    // 6. Fault Tolerance

    println("\n==================================================")
    println("FAULT TOLERANCE")
    println("==================================================")

    println("RDDs store lineage information.")
    println("If an executor loses a partition, Spark can")
    println("recompute that partition using its lineage.")

    println("\nIf an executor loses a partition:")

    println("1. Spark identifies the lost partition.")
    println("2. Spark looks at the RDD lineage.")
    println("3. Spark recomputes the lost partition.")
    println("4. Spark reruns the required transformations.")
    println("5. The missing partition is reconstructed.")

    // 7. Conceptual Executor Loss Simulation

    println("\n==================================================")
    println("CONCEPTUAL EXECUTOR LOSS SIMULATION")
    println("==================================================")

    println("Assume executor 2 loses a partition of 'squared'.")

    println("\nWhat Spark recomputes:")

    println("numbers partition")
    println("      |")
    println("      | map")
    println("      v")
    println("doubled partition")
    println("      |")
    println("      | filter")
    println("      v")
    println("evenNumbers partition")
    println("      |")
    println("      | map")
    println("      v")
    println("squared partition")

    println("\nSpark does NOT need to recompute unrelated")
    println("partitions that are still available.")

    // 8. Actions

    println("\n==================================================")
    println("ACTIONS")
    println("==================================================")

    println(s"Count: ${finalRDD.count()}")
    println(s"First element: ${finalRDD.first()}")
    println(s"First 3 elements: ${finalRDD.take(3).mkString(", ")}")

    sc.stop()

    println("\n==================================================")
    println("PROGRAM COMPLETED")
    println("==================================================")
  }
}
