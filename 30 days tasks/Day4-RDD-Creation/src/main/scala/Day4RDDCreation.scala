import org.apache.spark.{SparkConf, SparkContext}

object Day4RDDCreation {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day4 RDD Creation")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("===== RDD Creation from Collection =====")

    val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))

    println("Original RDD:")
    numbers.collect().foreach(println)

    println("\n===== map =====")

    val doubled = numbers.map(_ * 2)
    doubled.collect().foreach(println)

    println("\n===== filter =====")

    val evenNumbers = numbers.filter(_ % 2 == 0)
    evenNumbers.collect().foreach(println)

    println("\n===== flatMap =====")

    val words = sc.parallelize(Seq(
      "Apache Spark",
      "Scala Programming",
      "Big Data"
    ))

    val allWords = words.flatMap(_.split(" "))
    allWords.collect().foreach(println)

    println("\n===== RDD Creation from Text File =====")

    val fileRDD = sc.textFile("data/input.txt")

    println("File Contents:")
    fileRDD.collect().foreach(println)

    println("\n===== Total Sales =====")

    val transactions = sc.parallelize(Seq(
      "101,250.50",
      "102,100.00",
      "103,450.75",
      "104,199.25"
    ))

    val totalSales = transactions
      .map(line => line.split(",")(1).toDouble)
      .sum()

    println("Total Sales: " + totalSales)

    println("\n===== Partition Information =====")

    println("Number of partitions: " + numbers.getNumPartitions)
    println("Default parallelism: " + sc.defaultParallelism)

    println("\n===== Large Customer File Scenario =====")

    val customers = sc.textFile("data/customers.txt", 4)

    println("Customer file partitions: " + customers.getNumPartitions)

    customers
      .mapPartitionsWithIndex { (partitionId, records) =>
        Iterator("Partition " + partitionId + ": " + records.size + " records")
      }
      .collect()
      .foreach(println)

    sc.stop()
  }
}
