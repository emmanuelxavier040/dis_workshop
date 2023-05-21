package spark.streaming.features

import org.apache.spark.streaming.dstream.DStream
import spark.streaming.data.Ride
import spark.streaming.integration.kibana.KibanaOps

object EndStations {
  var totalRecords = 0.0

  def setTotal(total: Double): Unit = {
    totalRecords = total
  }

  def getTotal: Double = {
    totalRecords
  }

  val updateFunction = (newValues: Seq[Int], runningCount: Option[(Int, Int)]) => {
    val newCount = newValues.sum
    if(newCount > 0) {
      setTotal(getTotal + newCount.toDouble)
    }
    val totalCount = newCount + runningCount.map(_._1).getOrElse(0)
    val totalSum = newCount + runningCount.map(_._2).getOrElse(0)
    Some((totalCount, totalSum))
  }

  def percentageOfEndStations(stream: DStream[Ride]): Unit = {
    //setTotal(getTotal + 1.0)
    val totalCounts = stream.map(ride => (ride.end_station_name, 1)).updateStateByKey(updateFunction)
    val percentage = totalCounts.mapValues { case (count, sum) => (sum.toDouble / getTotal) * 100 }
    percentage.print()

    KibanaOps.sendAvgCountBikeTypeToELK(percentage)
  }

}
