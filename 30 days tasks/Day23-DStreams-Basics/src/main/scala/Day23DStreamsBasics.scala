import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day23DStreamsBasics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day23-DStreams-Basics")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val ssc = new StreamingContext(
      spark.sparkContext,
      Seconds(2)
    )

    println("=" * 65)
    println("DAY 23 - SPARK DSTREAMS BASICS")
    println(s"Spark version: ${spark.version}")
    println(s"Application ID: ${spark.sparkContext.applicationId}")
    println(s"Master: ${spark.sparkContext.master}")
    println("Micro-batch interval: 2 seconds")
    println("=" * 65)

    // Read incoming lines from a TCP socket.
    val lines = ssc.socketTextStream("localhost", 9999)

    // Transformation: split each line into words.
    val words = lines.flatMap(_.toLowerCase.split("\\\\s+"))

    // Transformation: remove empty words.
    val nonEmptyWords = words.filter(_.nonEmpty)

    // Transformation: count words in each micro-batch.
    val wordCounts = nonEmptyWords
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // Output action for each micro-batch.
    wordCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\\n--- WORD COUNTS FOR THIS BATCH ---")
        rdd.collect().sortBy(_._1).foreach(println)
      }
    }

    // Detect application log lines containing ERROR.
    val errorLines = lines.filter(
      line => line.toUpperCase.contains("ERROR")
    )

    errorLines.foreachRDD { rdd =>
      val count = rdd.count()

      if (count > 0) {
        println(s"\\n!!! ERROR LINES IN THIS BATCH: $count !!!")
        rdd.collect().foreach(line => println(s"ALERT: $line"))
      }
    }

    println("Starting streaming. Send text to localhost:9999.")
    println("The application will run for approximately 30 seconds.")

    ssc.start()
    ssc.awaitTerminationOrTimeout(30000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)

    println("=" * 65)
    println("DAY 23 COMPLETED SUCCESSFULLY")
    println("=" * 65)
  }
}
