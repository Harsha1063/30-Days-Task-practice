
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.Instant
import scala.util.Try

case class OrderEvent(
    eventId: String,
    orderId: String,
    customerId: String,
    productId: String,
    quantity: Int,
    unitPrice: Double,
    status: String,
    timestamp: Long
)

case class OrderState(
    customerId: String,
    productId: String,
    quantity: Int,
    unitPrice: Double,
    status: String,
    timestamp: Long
)

case class ProductSales(
    productId: String,
    totalQuantity: Long,
    totalRevenue: Double
)

object Day29ECommerce {

  def parseEvent(line: String): Option[OrderEvent] = {
    val fields = line.split(",", -1).map(_.trim)

    if (fields.length != 8) {
      None
    } else {
      val parsed = for {
        quantity <- Try(fields(4).toInt).toOption
        price <- Try(fields(5).toDouble).toOption
        timestamp <- Try(Instant.parse(fields(7)).toEpochMilli).toOption
      } yield OrderEvent(
        fields(0),
        fields(1),
        fields(2),
        fields(3),
        quantity,
        price,
        fields(6).toUpperCase,
        timestamp
      )

      parsed.filter { event =>
        event.eventId.nonEmpty &&
        event.orderId.nonEmpty &&
        event.customerId.nonEmpty &&
        event.productId.nonEmpty &&
        event.quantity > 0 &&
        event.unitPrice >= 0 &&
        !event.unitPrice.isNaN &&
        !event.unitPrice.isInfinity &&
        Set("PLACED", "CANCELLED", "COMPLETED").contains(event.status)
      }
    }
  }

  def updateOrder(
      events: Seq[OrderEvent],
      previous: Option[OrderState]
  ): Option[OrderState] = {

    val latest = events.sortBy(_.timestamp).lastOption

    latest match {
      case Some(event) =>
        previous match {
          case Some(old) if old.timestamp > event.timestamp =>
            Some(old)

          case _ =>
            Some(
              OrderState(
                event.customerId,
                event.productId,
                event.quantity,
                event.unitPrice,
                event.status,
                event.timestamp
              )
            )
        }

      case None => previous
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day29-E-Commerce-Streaming")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val sc = ssc.sparkContext
    sc.setLogLevel("WARN")

    val spark = SparkSession.builder()
      .config(sc.getConf)
      .getOrCreate()

    import spark.implicits._

    val events = ssc.socketTextStream("localhost", 9999)
      .flatMap(parseEvent)

    val orderStates = events
      .map(event => (event.orderId, event))
      .updateStateByKey[OrderState](updateOrder _)

    // Rolling 10-second completed-event sales window.
    events
      .filter(_.status == "COMPLETED")
      .map(event =>
        (event.productId,
          (event.quantity.toLong, event.quantity * event.unitPrice))
      )
      .reduceByKeyAndWindow(
        (a: (Long, Double), b: (Long, Double)) =>
          (a._1 + b._1, a._2 + b._2),
        (a: (Long, Double), b: (Long, Double)) =>
          (a._1 - b._1, a._2 - b._2),
        Seconds(10),
        Seconds(4)
      )
      .foreachRDD { rdd =>
        rdd.filter(_._2._1 > 0).collect().foreach {
          case (productId, totals) =>
            println(
              f"PRODUCT SALES: Product=$productId%s, Quantity=${totals._1}%d, Revenue=${totals._2}%.2f"
            )
        }
      }

    orderStates.foreachRDD { stateRDD =>

      val currentStates = stateRDD.collect()

      if (currentStates.nonEmpty) {

        val reportRows = currentStates.map {
          case (orderId, state) =>
            (
              orderId,
              state.customerId,
              state.productId,
              state.quantity,
              state.unitPrice,
              state.status,
              state.quantity * state.unitPrice
            )
        }

        val ordersDF = reportRows.toSeq.toDF(
          "orderId",
          "customerId",
          "productId",
          "quantity",
          "unitPrice",
          "status",
          "orderValue"
        )

        ordersDF.createOrReplaceTempView("orders")

        println("\nORDER STATUS REPORT")

        spark.sql("""
          SELECT status,
                 COUNT(*) AS orderCount,
                 ROUND(SUM(orderValue), 2) AS orderValue
          FROM orders
          GROUP BY status
          ORDER BY status
        """).show(false)

        val completedSales = currentStates
          .filter { case (_, state) => state.status == "COMPLETED" }
          .map { case (_, state) =>
            (
              state.productId,
              (state.quantity.toLong, state.quantity * state.unitPrice)
            )
          }
          .groupBy(_._1)
          .map { case (productId, values) =>
            ProductSales(
              productId,
              values.map(_._2._1).sum,
              values.map(_._2._2).sum
            )
          }
          .toSeq

        if (completedSales.nonEmpty) {
          println("COMPLETED SALES SUMMARY")
          spark.createDataset(completedSales).toDF().show(false)
        }

        currentStates.foreach { case (orderId, state) =>
          println(
            s"ORDER STATE: Order=$orderId, Customer=${state.customerId}, " +
            s"Product=${state.productId}, Status=${state.status}"
          )
        }
      }
    }

    println("Day 29 E-Commerce stream started on localhost:9999")

    ssc.start()
    ssc.awaitTermination()
  }
}

