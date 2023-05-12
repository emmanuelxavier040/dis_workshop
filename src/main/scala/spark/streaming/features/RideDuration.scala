package spark.streaming.features

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.data.{BikeTypeRideDuration, Ride, RideElastic, UserTypeRideDuration}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object RideDuration {

  def rideDuration(ride: Ride) : Long = {
    LocalDateTime.from(ride.started_at)
      .until(ride.ended_at, ChronoUnit.SECONDS)
  }

  def avgRideDuration(ride: Ride): Unit = {

  }


  def rideDurationForBikeType(stream: DStream[Ride]) = {
    stream.map(ride => BikeTypeRideDuration(ride.rideable_type, rideDuration(ride)))
  }


  def rideDurationForUserType(stream: DStream[Ride]) = {
    stream.map(ride => UserTypeRideDuration(ride.member_casual, rideDuration(ride)))
  }
}
