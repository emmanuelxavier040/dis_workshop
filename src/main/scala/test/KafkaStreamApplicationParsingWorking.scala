package test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaStreamApplicationParsingWorking extends App {
  val sparkConf = new SparkConf().setAppName("SparkStreamingApp").setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConf)
  sparkContext.setLogLevel("ERROR")
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "ec2-18-213-16-8.compute-1.amazonaws.com:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "my-next-group0", // change it for a new stream
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("my-new-topic")
  val stream = KafkaUtils.createDirectStream[String, String](
    sparkStreamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  //stream.map(record => (record.key, record.value))
  stream.foreachRDD(rdd => rdd.foreach { v => println(v) })
  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()

}

