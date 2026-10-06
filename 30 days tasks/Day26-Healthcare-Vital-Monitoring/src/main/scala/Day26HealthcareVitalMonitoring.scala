import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day26HealthcareVitalMonitoring {

  case class Vital(
      patientId: String,
      heartRate: Int,
      oxygen: Double,
      temperature: Double,
      timestamp: String
  )

  def parseVital(line: String): Option[Vital] = {
    try {
      val parts = line.trim.split(",", -1)

      if (parts.length != 5) {
        None
      } else {
        val patientId = parts(0).trim
        val heartRate = parts(1).trim.toInt
        val oxygen = parts(2).trim.toDouble
        val temperature = parts(3).trim.toDouble
        val timestamp = parts(4).trim

        if (patientId.isEmpty || timestamp.isEmpty ||
            heartRate < 0 || oxygen < 0 || oxygen > 100 ||
            temperature < 0) {
          None
        } else {
          Some(Vital(
            patientId, heartRate, oxygen, temperature, timestamp
          ))
        }
      }
    } catch {
      case _: Exception => None
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day26HealthcareVitalMonitoring")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Broadcast clinical alert thresholds.
    val thresholds = ssc.sparkContext.broadcast(
      Map(
        "heart_rate" -> 120.0,
        "oxygen" -> 90.0,
        "temperature" -> 39.0
      )
    )

    // Demonstration accumulator for abnormal readings.
    val abnormalAccumulator =
      ssc.sparkContext.longAccumulator("AbnormalVitalReadings")

    val input = ssc.socketTextStream("localhost", 9999)

    val vitals = input.flatMap(parseVital)

    val abnormal = vitals.filter { vital =>
      val limits = thresholds.value

      vital.heartRate > limits("heart_rate") ||
      vital.oxygen < limits("oxygen") ||
      vital.temperature > limits("temperature")
    }

    // Print individual abnormal readings and count them.
    abnormal.foreachRDD { rdd =>
      val records = rdd.map { vital =>
        abnormalAccumulator.add(1L)
        vital
      }.collect()

      records.foreach { vital =>
        val reasons = Seq(
          if (vital.heartRate > 120)
            Some("HIGH HEART RATE") else None,
          if (vital.oxygen < 90)
            Some("LOW OXYGEN") else None,
          if (vital.temperature > 39.0)
            Some("HIGH TEMPERATURE") else None
        ).flatten.mkString(", ")

        println(
          s"ALERT: Patient=${vital.patientId}, " +
          s"HR=${vital.heartRate}, O2=${vital.oxygen}, " +
          s"Temp=${vital.temperature}, " +
          s"Time=${vital.timestamp}, Reason=$reasons"
        )
      }

      if (records.nonEmpty) {
        println(
          s"Total abnormal readings processed: " +
          s"${abnormalAccumulator.value}"
        )
      }
    }

    // Rolling count of abnormal readings in the last 10 seconds.
    val rollingAlerts =
      abnormal.countByWindow(Seconds(10), Seconds(4))

    rollingAlerts.foreachRDD { rdd =>
      rdd.collect().foreach { count =>
        println(s"Abnormal readings in last 10 seconds: $count")
      }
    }

    // Stateful cumulative abnormal-reading count per patient.
    val patientCounts = abnormal
      .map(vital => (vital.patientId, 1))
      .updateStateByKey[Int](
        (newValues: Seq[Int], previous: Option[Int]) =>
          Some(newValues.sum + previous.getOrElse(0))
      )

    patientCounts.foreachRDD { rdd =>
      rdd.collect().foreach { case (patientId, count) =>
        println(
          s"STATEFUL COUNT: Patient=$patientId, " +
          s"CumulativeAbnormalReadings=$count"
        )
      }
    }

    println("Healthcare Vital Monitoring started on port 9999.")
    println("Send: patientId,heartRate,oxygen,temperature,timestamp")

    ssc.start()
    ssc.awaitTermination()
  }
}
