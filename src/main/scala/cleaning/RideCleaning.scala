package cleaning

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
object RideCleaning {
  // test command
  def isValidRide(v: String): Boolean = {

    try {
      parseToRide(v)
    } catch {
      case _: Throwable => return false
    }

    val record = parseToRide(v)

    if (record.ended_at.isBefore(record.started_at))
      return false

    if (record.rideable_type != null && record.rideable_type.contains("docked_bike"))
      return false

    true
  }

  def parseToRide(value: String): Ride = {

    val record = value.split(",")

    //val dateFormat = "dd/MM/yyyy HH:mm"
    val dateFormat = "yyyy-MM-dd HH:mm:ss"
    val formatter = DateTimeFormatter.ofPattern(dateFormat)

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
}
