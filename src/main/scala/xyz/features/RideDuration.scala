package xyz.features

import xyz.data.Ride

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

object RideDuration {

  def rideDuration(ride: Ride) : Long = {
    LocalDateTime.from(ride.started_at).until(ride.started_at, ChronoUnit.SECONDS)
  }

  def avgRideDuration(ride: Ride): Unit = {

  }

}
