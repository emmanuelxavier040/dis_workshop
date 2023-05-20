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

  val updateFunction = (newValues: Seq[Int], runningCount: Option[(Int, Int)]) => {
    val newCount = newValues.sum
    val totalCount = newCount + runningCount.map(_._1).getOrElse(0)
    val totalSum = newCount + runningCount.map(_._2).getOrElse(0)
    Some((totalCount, totalSum))
  }

  def averageNumberOfRides(stream: DStream[Ride]): Unit = {
    val totalCounts = stream.map(ride => (ride.rideable_type, 1)).updateStateByKey(updateFunction)
    val averageCounts = totalCounts.mapValues { case (count, sum) => sum.toDouble / count }
    KibanaOps.sendAvgCountBikeTypeToELK(averageCounts)
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


  /*def rideDurationForUserType(stream: DStream[Ride]): Unit = {
    val s = StartStations.percentageOfStartStations(stream)
    KibanaOps.sendStartStationCountsToELK(s)
  }*/

}
