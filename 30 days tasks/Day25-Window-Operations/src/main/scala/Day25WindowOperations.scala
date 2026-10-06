import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day25WindowOperations {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day25-Window-Operations")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("checkpoint")

    println("=" * 65)
    println("DAY 25 - SPARK STREAMING WINDOW OPERATIONS")
    println("Batch interval: 2 seconds")
    println("Window duration: 10 seconds")
    println("Sliding interval: 4 seconds")
    println("Input format: transaction_id,product,amount")
    println("Connect using: nc -lk 9999")
    println("=" * 65)

    val lines = ssc.socketTextStream("localhost", 9999)

    val transactions = lines.flatMap { line =>
      val fields = line.split(",").map(_.trim)

      if (fields.length == 3) {
        try {
          val transactionId = fields(0)
          val product = fields(1)
          val amount = fields(2).toDouble

          if (transactionId.nonEmpty && product.nonEmpty && amount > 0)
            Some((transactionId, product, amount))
          else
            None
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // 1. Count transactions in the rolling 10-second window.
    val transactionCount = transactions.countByWindow(
      Seconds(10),
      Seconds(4)
    )

    transactionCount.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n--- TRANSACTION COUNT IN LAST 10 SECONDS ---")
        rdd.collect().foreach(count => println(s"Transactions: $count"))
      }
    }

    // 2. Calculate rolling sales totals per product.
    val productSales = transactions.map {
      case (_, product, amount) => (product, amount)
    }

    val rollingSales = productSales.reduceByKeyAndWindow(
      (a: Double, b: Double) => a + b,
      Seconds(10),
      Seconds(4)
    )

    rollingSales.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n--- ROLLING SALES TOTALS (LAST 10 SECONDS) ---")
        rdd.collect().sortBy(_._1).foreach {
          case (product, total) =>
            println(f"$product%-15s Total sales: $total%.2f")
        }
      }
    }

    // 3. Detect transaction bursts within the rolling window.
    val burstThreshold = 3

    transactionCount.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.collect().foreach { count =>
          if (count >= burstThreshold) {
            println(
              s"ALERT: Transaction burst detected! " +
              s"$count transactions in the last 10 seconds."
            )
          }
        }
      }
    }

    ssc.start()
    println("\nStreaming started. Send transaction records to port 9999.")
    ssc.awaitTerminationOrTimeout(60000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)

    println("\nDAY 25 COMPLETED")
  }
}
