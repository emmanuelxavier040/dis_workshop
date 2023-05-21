package spark.streaming.integration.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import java.util.Properties

object KafkaOps {


  def kafkaParams(): Map[String, Object] = {
//    val properties = new Properties()
//    val inputStream = getClass.getResourceAsStream("src/main/resources/app.properties")
//    properties.load(inputStream)
//    val propertyValue = properties.getProperty("bootstrap.servers")
//    println(propertyValue)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ec2-18-213-16-8.compute-1.amazonaws.com:9092",
//        "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafkaParams
  }

  def createKafkaStream(sparkStreamingContext: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = KafkaOps.kafkaParams()
    val topics = Array("ride-topic")
//    val topics = Array("my-new-topic")
//    val topics = Array("test-filter")
//val topics = Array("my-topic")
    val stream = KafkaUtils.createDirectStream[String, String](
      sparkStreamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }



}
