import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.Instant
import scala.util.Try

case class Transaction(
    transactionId: String,
    accountId: String,
    branchId: String,
    amount: Double,
    transactionType: String,
    timestamp: Long,
    branchRisk: String = "UNKNOWN"
)

object Day27Banking {

  private def parseTransaction(line: String): Option[Transaction] = {
    val fields = line.split(",", -1).map(_.trim)

    if (fields.length != 6) {
      None
    } else {
      val parsed = for {
        amount <- Try(fields(3).toDouble).toOption
        timestamp <- Try(Instant.parse(fields(5)).toEpochMilli).toOption
      } yield {
        Transaction(
          transactionId = fields(0),
          accountId = fields(1),
          branchId = fields(2),
          amount = amount,
          transactionType = fields(4),
          timestamp = timestamp
        )
      }

      parsed.filter { tx =>
        tx.transactionId.nonEmpty &&
        tx.accountId.nonEmpty &&
        tx.branchId.nonEmpty &&
        tx.amount > 0 &&
        tx.transactionType.nonEmpty
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Day27-Real-Time-Banking")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("checkpoint")
    ssc.sparkContext.setLogLevel("WARN")

    // Small branch-risk reference data shared with executors.
    val branchRisk = ssc.sparkContext.broadcast(
      Map(
        "BR001" -> "LOW",
        "BR002" -> "HIGH",
        "BR003" -> "MEDIUM"
      )
    )

    val input = ssc.socketTextStream("localhost", 9999)

    val transactions = input.flatMap(parseTransaction)

    // Enrich transactions using the broadcast reference map.
    val enriched = transactions.map { tx =>
      tx.copy(
        branchRisk =
          branchRisk.value.getOrElse(tx.branchId, "UNKNOWN")
      )
    }

    // Cache each micro-batch and repartition by account for processing.
    enriched.foreachRDD { rdd =>
      val cached = rdd.persist(StorageLevel.MEMORY_ONLY)

      try {
        val total = cached.count()

        if (total > 0) {
          val partitioned = cached
            .keyBy(_.accountId)
            .partitionBy(new HashPartitioner(4))

          println(s"\nTRANSACTIONS IN CURRENT BATCH: $total")
          println(s"ACCOUNT PARTITIONS: ${partitioned.getNumPartitions}")

          partitioned.collect().foreach {
            case (account, tx) =>
              println(
                s"TRANSACTION: ID=${tx.transactionId}, " +
                s"Account=$account, Branch=${tx.branchId}, " +
                s"Risk=${tx.branchRisk}, Amount=${tx.amount}, " +
                s"Type=${tx.transactionType}"
              )
          }
        }
      } finally {
        cached.unpersist(false)
      }
    }

    // Aggregate transaction counts by account in each micro-batch.
    val accountCounts = enriched
      .map(tx => (tx.accountId, 1))
      .reduceByKey(_ + _)

    accountCounts.foreachRDD { rdd =>
      val results = rdd.collect()

      if (results.nonEmpty) {
        println("\nACCOUNT-WISE TRANSACTION COUNTS")
        results.foreach {
          case (account, count) =>
            println(s"Account=$account, Transactions=$count")
        }
      }
    }

    // Detect three or more transactions per account in a rolling
    // 10-second window, updated every 4 seconds.
    val burstCounts = transactions
      .map(tx => (tx.accountId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        (a: Int, b: Int) => a - b,
        Seconds(10),
        Seconds(4)
      )
      .filter { case (_, count) => count >= 3 }

    burstCounts.foreachRDD { rdd =>
      val alerts = rdd.collect()

      alerts.foreach {
        case (account, count) =>
          println(
            s"FRAUD ALERT: Account=$account has $count " +
            "transactions in the last 10 seconds!"
          )
      }
    }

    // Flag transactions associated with high-risk branches.
    enriched
      .filter(_.branchRisk == "HIGH")
      .foreachRDD { rdd =>
        rdd.collect().foreach { tx =>
          println(
            s"RISK ALERT: Transaction=${tx.transactionId}, " +
            s"Account=${tx.accountId}, Branch=${tx.branchId}, " +
            s"Amount=${tx.amount}"
          )
        }
      }

    println("Day 27 Banking Stream started on localhost:9999")
    ssc.start()
    ssc.awaitTermination()
  }
}
