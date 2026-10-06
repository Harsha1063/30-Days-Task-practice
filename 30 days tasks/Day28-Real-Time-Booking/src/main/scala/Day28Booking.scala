import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.Instant
import scala.util.Try

case class BookingEvent(
    eventId: String,
    bookingId: String,
    hotelId: String,
    rooms: Int,
    eventType: String,
    timestamp: Long
)

case class BookingState(
    hotelId: String,
    rooms: Int,
    status: String,
    timestamp: Long
)

case class Occupancy(
    hotelId: String,
    bookedRooms: Int,
    capacity: Int,
    availableRooms: Int
)

object Day28Booking {

  def parseEvent(line: String): Option[BookingEvent] = {
    val fields = line.split(",", -1).map(_.trim)

    if (fields.length != 6) {
      None
    } else {
      val parsed = for {
        rooms <- Try(fields(3).toInt).toOption
        timestamp <- Try(Instant.parse(fields(5)).toEpochMilli).toOption
      } yield BookingEvent(
        fields(0),
        fields(1),
        fields(2),
        rooms,
        fields(4).toUpperCase,
        timestamp
      )

      parsed.filter { event =>
        event.eventId.nonEmpty &&
        event.bookingId.nonEmpty &&
        event.hotelId.nonEmpty &&
        event.rooms > 0 &&
        Set("BOOK", "CANCEL").contains(event.eventType)
      }
    }
  }

  def updateBooking(
      events: Seq[BookingEvent],
      previous: Option[BookingState]
  ): Option[BookingState] = {

    val latest = events.sortBy(_.timestamp).lastOption

    latest match {
      case Some(event) =>
        previous match {
          case Some(old) if old.timestamp > event.timestamp =>
            Some(old)

          case _ =>
            Some(
              BookingState(
                event.hotelId,
                event.rooms,
                event.eventType,
                event.timestamp
              )
            )
        }

      case None => previous
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day28-Real-Time-Booking")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val sc = ssc.sparkContext
    sc.setLogLevel("WARN")

    val spark = SparkSession.builder()
      .config(sc.getConf)
      .getOrCreate()

    import spark.implicits._

    val capacityBroadcast = sc.broadcast(
      Map(
        "HOTEL101" -> 100,
        "HOTEL102" -> 60,
        "HOTEL103" -> 80
      )
    )

    val events = ssc.socketTextStream("localhost", 9999)
      .flatMap(parseEvent)

    // Maintain the latest state for each booking.
    val bookingStates = events
      .map(event => (event.bookingId, event))
      .updateStateByKey[BookingState](updateBooking _)

    // Count incoming booking events in a rolling 10-second window.
    events
      .map(event => (event.hotelId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        (a: Int, b: Int) => a - b,
        Seconds(10),
        Seconds(4)
      )
      .foreachRDD { rdd =>
        rdd.collect().foreach {
          case (hotelId, count) =>
            println(
              s"BOOKING WINDOW: Hotel=$hotelId, EventsInLast10Seconds=$count"
            )
        }
      }

    bookingStates.foreachRDD { stateRDD =>

      val currentStates = stateRDD.collect()

      if (currentStates.nonEmpty) {

        // Only currently booked reservations count toward occupancy.
        val confirmed = currentStates
          .filter { case (_, state) => state.status == "BOOK" }
          .map { case (_, state) => (state.hotelId, state.rooms) }
          .groupBy(_._1)
          .map { case (hotelId, entries) =>
            (hotelId, entries.map(_._2).sum)
          }

        // Demonstrate partitioning of hotel occupancy data.
        val occupancyRDD = sc.parallelize(confirmed.toSeq)
          .partitionBy(new HashPartitioner(4))

        val occupancyRows = occupancyRDD.collect().map {
          case (hotelId, bookedRooms) =>
            val capacity = capacityBroadcast.value.getOrElse(hotelId, 0)

            Occupancy(
              hotelId,
              bookedRooms,
              capacity,
              math.max(0, capacity - bookedRooms)
            )
        }

        if (occupancyRows.nonEmpty) {
          val occupancyDF = spark.createDataset(occupancyRows).toDF()
          occupancyDF.createOrReplaceTempView("hotel_occupancy")

          println("\nHOTEL OCCUPANCY REPORT")

          spark.sql("""
            SELECT hotelId,
                   bookedRooms,
                   capacity,
                   availableRooms,
                   CASE
                     WHEN capacity = 0 THEN 0.0
                     ELSE ROUND(bookedRooms * 100.0 / capacity, 2)
                   END AS occupancyPercent
            FROM hotel_occupancy
            ORDER BY hotelId
          """).show(false)
        }

        currentStates.foreach { case (bookingId, state) =>
          println(
            s"BOOKING STATE: Booking=$bookingId, Hotel=${state.hotelId}, " +
            s"Rooms=${state.rooms}, Status=${state.status}"
          )
        }
      }
    }

    println("Day 28 Booking Stream started on localhost:9999")

    ssc.start()
    ssc.awaitTermination()
  }
}
