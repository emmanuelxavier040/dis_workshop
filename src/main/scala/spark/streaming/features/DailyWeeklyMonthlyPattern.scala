package spark.streaming.features

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.data.{BikeTypeRideDuration, Ride, UserTypeRideDuration}

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

object DailyWeeklyMonthlyPattern {

  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
  import java.time.LocalDate
  import java.time.format.DateTimeFormatter

    /*def dailyPattern(stream: DStream[Ride]) = {
      stream.map(ride => DailyPatternBike(ride.rideable_type, ride.started_at))

      stream.foreachRDD(rdd => {
        val currentDate = LocalDate.now()
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

        val dailyCounts = rdd.filter { case (rdd.rideable_type, _) => rdd.rideable_type == "electric" }
          .filter { case (_, rdd.started_at) =>
            val rideDate = LocalDate.parse(timestamp.toString, dateFormatter)
            rideDate.isEqual(currentDate)
          }
          .count()
        println(s"Daily electric bike rides: $dailyCounts")

      })
    }*/

      /*val bikeRides = rideData.map(line => {
        val fields = line.split(",")
        val rideType = fields(0)
        val timestamp = fields(1).toLong
        (rideType, timestamp)
      })

      // Calculate usage patterns
      bikeRides.foreachRDD(rdd => {
        val currentDate = LocalDate.now()
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

        // Daily pattern
        val dailyCounts = rdd.filter { case (rideType, _) => rideType == "electric" }
          .filter { case (_, timestamp) =>
            val rideDate = LocalDate.parse(timestamp.toString, dateFormatter)
            rideDate.isEqual(currentDate)
          }
          .count()

        // Weekly pattern
        val weeklyCounts = rdd.filter { case (rideType, _) => rideType == "electric" }
          .filter { case (_, timestamp) =>
            val rideDate = LocalDate.parse(timestamp.toString, dateFormatter)
            rideDate.isAfter(currentDate.minusDays(7))
          }
          .count()

        // Monthly pattern
        val monthlyCounts = rdd.filter { case (rideType, _) => rideType == "electric" }
          .filter { case (_, timestamp) =>
            val rideDate = LocalDate.parse(timestamp.toString, dateFormatter)
            rideDate.isAfter(currentDate.minusMonths(1))
          }
          .count()

        println(s"Daily electric bike rides: $dailyCounts")
        println(s"Weekly electric bike rides: $weeklyCounts")
        println(s"Monthly electric bike rides: $monthlyCounts")
      })

      ssc.start()
      ssc.awaitTermination()
    }*/


}
