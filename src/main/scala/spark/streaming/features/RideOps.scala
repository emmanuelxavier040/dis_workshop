package spark.streaming.features

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.data.{BikeTypeRideDuration, Ride, RideCleaning, RideElastic, UserTypeRideDuration}
import spark.streaming.integration.kibana.KibanaOps

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

object RideOps {

  def rideDuration(ride: Ride) : Long = {
    LocalDateTime.from(ride.started_at)
      .until(ride.ended_at, ChronoUnit.SECONDS)
  }

  def avgRideDuration(ride: Ride): Unit = {

  }


  def allRides(stream: DStream[Ride]): Unit = {
    val elasticRideStream =  stream.map(ride => RideCleaning.parseToRideForElastic(ride))
    KibanaOps.sendRidesToELK(elasticRideStream)
  }


  def rideDurationForBikeType(stream: DStream[Ride]): Unit = {
    val s = stream.map(ride => BikeTypeRideDuration(ride.rideable_type, rideDuration(ride)))
    KibanaOps.sendRideDurationBikeTypeToELK(s)
  }


  def rideDurationForUserType(stream: DStream[Ride]): Unit = {
    val s = stream.map(ride => UserTypeRideDuration(ride.member_casual, rideDuration(ride)))
    KibanaOps.sendRideDurationUserTypeToELK(s)
  }
}
