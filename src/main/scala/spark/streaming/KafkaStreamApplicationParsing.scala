package spark.streaming

import integration.kafka.KafkaOps
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import spark.streaming.data.RideCleaning
import spark.streaming.features.EndStations.percentageOfEndStations
import spark.streaming.features.{EndStations, RideOps, StartStations}
import spark.streaming.features.StartStations.percentageOfStartStations
import spark.streaming.integration.kibana.KibanaOps


object KafkaStreamApplicationParsing extends App {

  val sparkConf = new SparkConf().setAppName("SparkStreamingApp").setMaster("local[4]")

  val sparkContext = new SparkContext(sparkConf)
  sparkContext.setLogLevel("ERROR")
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))
  sparkStreamingContext.checkpoint("checkpoint_directory")

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaOps.createKafkaStream(sparkStreamingContext)

  val cleanedStream = stream.filter(x => RideCleaning.isValidRide(x.value))
  val rideStream = cleanedStream.map(x => RideCleaning.parseToRide(x.value))

  /*rideStream.foreachRDD { rdd =>
    rdd.foreach { v => println(v)}
  }*/

  // test for start stations:
  StartStations.setTotal(0.0) // without regarding the first line - after testing back to 0.0
  percentageOfStartStations(rideStream)

  // test for end stations:
  EndStations.setTotal(0.0) // without regarding the first line - after testing back to 0.0
  percentageOfEndStations(rideStream)

  //RideOps.allRides(rideStream)
  //RideOps.rideDurationForBikeType(rideStream)
  //RideOps.rideDurationForUserType(rideStream)
  //RideOps.averageNumberOfRides(rideStream)



  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()

}

