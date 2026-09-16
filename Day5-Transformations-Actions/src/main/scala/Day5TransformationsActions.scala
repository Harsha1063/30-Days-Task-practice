import org.apache.spark.{SparkConf, SparkContext}

object Day5TransformationsActions {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 5 - Transformations and Actions")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    println("==========================================")
    println("DAY 5 - TRANSFORMATIONS AND ACTIONS")
    println("==========================================")

    // ------------------------------------------------
    // 1. Create RDD from collection
    // ------------------------------------------------

    val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5))

    println("\nOriginal RDD:")
    println(numbers.collect().mkString(", "))


    // ------------------------------------------------
    // 2. map() - Transformation
    // ------------------------------------------------

    val doubled = numbers.map(x => x * 2)

    println("\nmap() - Double each number:")
    println(doubled.collect().mkString(", "))


    // ------------------------------------------------
    // 3. filter() - Transformation
    // ------------------------------------------------

    val evenNumbers = numbers.filter(x => x % 2 == 0)

    println("\nfilter() - Even numbers:")
    println(evenNumbers.collect().mkString(", "))


    // ------------------------------------------------
    // 4. flatMap() - Transformation
    // ------------------------------------------------

    val sentences = sc.parallelize(
      Seq(
        "Apache Spark",
        "Scala Programming",
        "Big Data"
      )
    )

    val words = sentences.flatMap(sentence => sentence.split(" "))

    println("\nflatMap() - Words:")
    println(words.collect().mkString(", "))


    // ------------------------------------------------
    // 5. distinct() - Transformation
    // ------------------------------------------------

    val duplicateNumbers = sc.parallelize(
      Seq(1, 2, 2, 3, 3, 3, 4, 5, 5)
    )

    val uniqueNumbers = duplicateNumbers.distinct()

    println("\ndistinct() - Unique numbers:")
    println(uniqueNumbers.collect().mkString(", "))


    // ------------------------------------------------
    // 6. union() - Transformation
    // ------------------------------------------------

    val rdd1 = sc.parallelize(Seq(1, 2, 3))
    val rdd2 = sc.parallelize(Seq(4, 5, 6))

    val combinedRDD = rdd1.union(rdd2)

    println("\nunion() - Combined RDD:")
    println(combinedRDD.collect().mkString(", "))


    // ------------------------------------------------
    // 7. count() - Action
    // ------------------------------------------------

    val numberCount = numbers.count()

    println("\ncount() - Number of elements:")
    println(numberCount)


    // ------------------------------------------------
    // 8. collect() - Action
    // ------------------------------------------------

    println("\ncollect() - All elements:")
    println(numbers.collect().mkString(", "))


    // ------------------------------------------------
    // 9. first() - Action
    // ------------------------------------------------

    println("\nfirst() - First element:")
    println(numbers.first())


    // ------------------------------------------------
    // 10. take() - Action
    // ------------------------------------------------

    println("\ntake(3) - First three elements:")
    println(numbers.take(3).mkString(", "))


    // ------------------------------------------------
    // 11. reduce() - Action
    // ------------------------------------------------

    val sum = numbers.reduce((a, b) => a + b)

    println("\nreduce() - Sum:")
    println(sum)


    // ------------------------------------------------
    // 12. LOG ANALYZER
    // Scenario: Count ERROR messages
    // ------------------------------------------------

    println("\n==========================================")
    println("LOG ANALYZER")
    println("==========================================")

    val logRDD = sc.textFile("data/application.log")

    println("\nTotal log records:")
    println(logRDD.count())


    // Filter only ERROR messages
    val errorLogs = logRDD.filter(line => line.startsWith("ERROR"))

    println("\nERROR messages:")
    errorLogs.collect().foreach(println)


    // Count ERROR messages
    val errorCount = errorLogs.count()

    println("\nTotal ERROR messages:")
    println(errorCount)


    // ------------------------------------------------
    // 13. flatMap() on log file
    // ------------------------------------------------

    val logWords = logRDD.flatMap(line => line.split(" "))

    println("\nTotal words in log:")
    println(logWords.count())


    // ------------------------------------------------
    // 14. distinct() on ERROR messages
    // ------------------------------------------------

    val distinctErrors = errorLogs.distinct()

    println("\nDistinct ERROR messages:")
    distinctErrors.collect().foreach(println)


    // ------------------------------------------------
    // Stop Spark
    // ------------------------------------------------

    sc.stop()

    println("\n==========================================")
    println("PROGRAM COMPLETED")
    println("==========================================")
  }
}
