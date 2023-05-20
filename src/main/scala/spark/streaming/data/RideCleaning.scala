package spark.streaming.data

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.WeekFields
import java.util.Locale

import spark.streaming.features.RideOps

object RideCleaning {

  def isValidRide(v : String): Boolean = {
    if(v.contains("started_at"))
      return false
    try {
      parseToRide(v)
    } catch  { case _: Throwable => {
      println("Discarding the Invalid ride => " + v)
      return false }
    }

    val record = parseToRide(v)

    if(record.ended_at.isBefore(record.started_at))
      return false

    if(record.rideable_type != null && record.rideable_type.contains("docked_bike"))
      return false

    if (record.start_station_id == null || record.start_station_id.contentEquals(""))
      return false

    if (record.end_station_id == null || record.end_station_id.contentEquals(""))
      return false

    true
  }

  def parseToRide(value: String): Ride = {
//    val dateFormat = "dd/MM/yyyy HH:mm"
    val dateFormat = "yyyy-MM-dd HH:mm:ss"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val record = value.split(",")
    Ride(
      record.lift(0).get,
      record.lift(1).get,
      LocalDateTime.parse(record.lift(2).get, formatter),
      LocalDateTime.parse(record.lift(3).get, formatter),
      record.lift(4).get,
      record.lift(5).get,
      record.lift(6).get,
      record.lift(7).get,
      if (record.lift(8).get.contentEquals("")) 0.0 else record.lift(8).get.toDouble,
      if (record.lift(9).get.contentEquals("")) 0.0 else record.lift(9).get.toDouble,
      if (record.lift(10).get.contentEquals("")) 0.0 else record.lift(10).get.toDouble,
      if (record.lift(11).get.contentEquals("")) 0.0 else record.lift(11).get.toDouble,
      record.lift(12).get)
  }

  def parseToRideForElastic(ride: Ride): RideElastic = {
    val weekFields = WeekFields.of(Locale.getDefault());
    val weekNumber = ride.started_at.get(weekFields.weekOfWeekBasedYear());

    RideElastic(
      ride.ride_id,
      ride.rideable_type,
      ride.started_at.toString,
      ride.ended_at.toString,
      ride.start_station_id,
      ride.start_station_name,
      ride.end_station_id,
      ride.end_station_name,
      ride.start_lat,
      ride.start_lng,
      ride.end_lat,
      ride.end_lng,
      ride.member_casual,
      ride.started_at.getHour.toString,
      ride.started_at.getDayOfWeek.toString,
      weekNumber.toString,
      ride.started_at.getMonthValue.toString,
      RideOps.rideDuration(ride)

    )
  }


}
