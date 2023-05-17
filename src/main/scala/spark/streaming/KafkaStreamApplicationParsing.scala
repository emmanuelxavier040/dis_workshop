package spark.streaming

import integration.kafka.KafkaOps
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import spark.streaming.data.RideCleaning
import spark.streaming.features.RideOps


object KafkaStreamApplicationParsing extends App {

  val sparkConf = new SparkConf().setAppName("SparkStreamingApp").setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConf)
  sparkContext.setLogLevel("ERROR")
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaOps.createKafkaStream(sparkStreamingContext)

  val cleanedStream = stream.filter(x => RideCleaning.isValidRide(x.value))
  val rideStream = cleanedStream.map(x => RideCleaning.parseToRide(x.value))



  RideOps.allRides(rideStream)
  RideOps.rideDurationForBikeType(rideStream)
  RideOps.rideDurationForUserType(rideStream)

  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()

}

