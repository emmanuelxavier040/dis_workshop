package spark.streaming.features

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.data.{BikeTypeRideDuration, Ride, RideElastic, UserTypeRideDuration}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object Patterns {

  def rideDuration(ride: RideElastic) : Long = {
    println(ride.started_at)
    val dateFormat = "yyyy-MM-dd'T'HH:mm"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    LocalDateTime.from(LocalDateTime.parse(ride.started_at, formatter))
      .until( LocalDateTime.parse(ride.ended_at, formatter), ChronoUnit.SECONDS)
  }

  def avgRideDuration(ride: Ride): Unit = {

  }


  def rideDurationForBikeType(stream: DStream[RideElastic]) = {
    stream.map(ride => BikeTypeRideDuration(ride.rideable_type, rideDuration(ride)))
  }


  def rideDurationForUserType(stream: DStream[RideElastic]) = {
    stream.map(ride => UserTypeRideDuration(ride.member_casual, rideDuration(ride)))
  }
}
