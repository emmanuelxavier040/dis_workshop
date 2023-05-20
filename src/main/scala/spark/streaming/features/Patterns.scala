package spark.streaming.features

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import spark.streaming.data.{BikeTypeRideDuration, Ride, RideElastic, UserTypeRideDuration}

import java.time.{LocalDate}
import java.time.format.DateTimeFormatter

object Patterns {


  def ridePattern(rideStream: DStream[Ride]): Unit = {

    // Calculate usage patterns
    rideStream.foreachRDD(rdd => {
      val currentDate = LocalDate.now()
      val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

      // Daily pattern
      val dailyCounts = rdd.filter { case ride => ride.rideable_type == "electric" }
        .filter { case ride =>
          val rideDate = LocalDate.parse(ride.started_at.toString, dateFormatter)
          rideDate.isEqual(currentDate)
        }
        .count()

      // Weekly pattern
      val weeklyCounts = rdd.filter { case ride => ride.rideable_type == "electric" }
        .filter { case ride =>
          val rideDate = LocalDate.parse(ride.started_at.toString, dateFormatter)
          rideDate.isAfter(currentDate.minusDays(7))
        }
        .count()

      // Monthly pattern
      val monthlyCounts = rdd.filter { case ride => ride.rideable_type == "electric" }
        .filter { case ride =>
          val rideDate = LocalDate.parse(ride.started_at.toString, dateFormatter)
          rideDate.isAfter(currentDate.minusMonths(1))
        }
        .count()

      println(s"Daily electric bike rides: $dailyCounts")
      println(s"Weekly electric bike rides: $weeklyCounts")
      println(s"Monthly electric bike rides: $monthlyCounts")
    })
  }
}
