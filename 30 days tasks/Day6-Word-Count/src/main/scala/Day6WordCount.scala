import org.apache.spark.{SparkConf, SparkContext}

object Day6WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 6 - Word Count")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    println("==================================================")
    println("DAY 6 - WORD COUNT")
    println("==================================================")

    // --------------------------------------------------
    // 1. Classic Word Count
    // flatMap -> map -> reduceByKey
    // --------------------------------------------------

    val textRDD = sc.parallelize(
      Seq(
        "Apache Spark is powerful",
        "Spark is fast",
        "Spark is used for big data"
      )
    )

    val words = textRDD
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    println("\nClassic Word Count:")
    words.collect().sortBy(_._1).foreach {
      case (word, count) =>
        println(s"$word -> $count")
    }

    // --------------------------------------------------
    // 2. Case-Insensitive Word Count
    // Ignore punctuation and empty words
    // --------------------------------------------------

    val cleanWords = textRDD
      .flatMap(line => line.toLowerCase.split("[^a-z0-9]+"))
      .filter(word => word.nonEmpty)

    val caseInsensitiveCounts = cleanWords
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    println("\nCase-Insensitive Word Count:")
    caseInsensitiveCounts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(s"$word -> $count")
      }

    // --------------------------------------------------
    // 3. Application Log Word Count
    // --------------------------------------------------

    println("\n==================================================")
    println("APPLICATION LOG WORD COUNT")
    println("==================================================")

    val logRDD = sc.textFile("data/application.log")

    val logWords = logRDD
      .flatMap(line => line.toLowerCase.split("[^a-z0-9]+"))
      .filter(word => word.nonEmpty)

    val logWordCounts = logWords
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    println("\nAll Log Word Counts:")
    logWordCounts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(s"$word -> $count")
      }

    // --------------------------------------------------
    // 4. Top 10 Most Frequent Words
    // --------------------------------------------------

    val top10Words = logWordCounts
      .sortBy(_._2, ascending = false)
      .take(10)

    println("\nTop 10 Most Frequent Words:")
    top10Words.zipWithIndex.foreach {
      case ((word, count), index) =>
        println(s"${index + 1}. $word -> $count")
    }

    sc.stop()

    println("\n==================================================")
    println("PROGRAM COMPLETED")
    println("==================================================")
  }
}
