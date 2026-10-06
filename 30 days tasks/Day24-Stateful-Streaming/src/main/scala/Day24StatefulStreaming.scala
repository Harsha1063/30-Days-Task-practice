import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day24StatefulStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day24-Stateful-Streaming")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val checkpointPath = "checkpoint"

    val ssc = new StreamingContext(
      spark.sparkContext,
      Seconds(2)
    )

    ssc.checkpoint(checkpointPath)

    println("=" * 65)
    println("DAY 24 - STATELESS VS STATEFUL STREAMING")
    println(s"Spark version: ${spark.version}")
    println("Batch interval: 2 seconds")
    println("Input format: account_id,transaction_amount")
    println("=" * 65)

    val lines = ssc.socketTextStream("localhost", 9999)

    val transactions = lines.flatMap { line =>
      val fields = line.split(",")

      if (fields.length == 2) {
        val account = fields(0).trim
        val amountText = fields(1).trim

        try {
          val amount = amountText.toDouble
          if (account.nonEmpty && amount > 0)
            Some((account, amount))
          else None
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // Stateless: count transactions in each individual micro-batch.
    val currentBatchCounts = transactions
      .map { case (account, _) => (account, 1) }
      .reduceByKey(_ + _)

    currentBatchCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\\n--- STATELESS: CURRENT BATCH COUNTS ---")
        rdd.collect().sortBy(_._1).foreach(println)
      }
    }

    // Stateful: accumulate transaction counts across batches.
    val accountEvents = transactions
      .map { case (account, _) => (account, 1) }

    val runningCounts = accountEvents.updateStateByKey[Int] {
      (newValues: Seq[Int], previousState: Option[Int]) =>
        Some(newValues.sum + previousState.getOrElse(0))
    }

    runningCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\\n--- STATEFUL: RUNNING TRANSACTION COUNTS ---")
        rdd.collect().sortBy(_._1).foreach(println)
      }
    }

    println("Listening on localhost:9999 for 40 seconds.")
    println("Example input: A001,100")

    ssc.start()
    ssc.awaitTerminationOrTimeout(40000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)

    println("=" * 65)
    println("DAY 24 COMPLETED SUCCESSFULLY")
    println("=" * 65)
  }
}
