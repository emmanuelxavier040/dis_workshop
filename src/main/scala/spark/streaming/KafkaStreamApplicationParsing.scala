package spark.streaming

import integration.kafka.KafkaOps
import integration.kibana.KibanaOps
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions
import xyz.data.RideCleaning

object KafkaStreamApplicationParsing extends App {

  val sparkConf = new SparkConf().setAppName("SparkStreamingApp").setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConf)
  sparkContext.setLogLevel("ERROR")
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaOps.createKafkaStream(sparkStreamingContext)

  val cleanedStream = stream.filter(x => RideCleaning.isValidRide(x.value))
  val objectStream = cleanedStream.map(x => RideCleaning.parseToRide(x.value))

  KibanaOps.sendToELK(objectStream)

//    objectStream.foreachRDD(rdd => rdd.foreach { v => println(v) })
  //objectStream.foreachRDD(rdd => rdd.foreach { v => println(RideDuration.rideDuration(v)) })

  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()

}

