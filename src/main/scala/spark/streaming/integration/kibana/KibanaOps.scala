package spark.streaming.integration.kibana

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sparkRDDFunctions
import spark.streaming.data.{BikeTypeRideDuration, RideElastic, UserTypeRideDuration}

object KibanaOps {

  val esNodes = "34.193.132.148:9200"

  val esConfig = Map(
    "es.nodes" -> esNodes,
    "es.port" -> "9200",
    "es.net.http.auth.user" -> "elastic",
    "es.net.http.auth.pass" -> "l6tyvxoqQR2HsJLMjliK",
    "es.nodes.wan.only" -> "true"
  )

  def sendRidesToELK(stream: DStream[RideElastic]): Unit = {
    val config = esConfig + ("es.resource" -> s"rides")
    stream.foreachRDD { rdd =>  rdd.saveToEs(config) }
  }

  def sendRideToELK(v : RDD[RideElastic]): Unit = {

  }


  def sendRideDurationBikeTypeToELK(stream: DStream[BikeTypeRideDuration]): Unit = {
    val config  = esConfig + ( "es.resource" -> s"ride_duration_bike_type")
    stream.foreachRDD { rdd => rdd.saveToEs(config) }
  }

  def sendRideDurationUserTypeToELK(stream: DStream[UserTypeRideDuration]): Unit = {
    val config  = esConfig+ ( "es.resource" -> s"ride_duration_user_type")
    stream.foreachRDD {rdd =>  rdd.saveToEs(config) }
  }

  def sendAvgCountBikeTypeToELK(stream: DStream[(String, Double)]): Unit = {
    val config = esConfig + ("es.resource" -> s"avg_count_bike_type")
    stream.foreachRDD { rdd => rdd.saveToEs(config) }
  }



}
