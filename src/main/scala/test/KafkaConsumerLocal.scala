package test



import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import scala.collection.JavaConverters._

object KafkaConsumerLocal extends App {

  import java.util.Properties

  val TOPIC = "my-new-topic"

  val props = new Properties()
  props.put("bootstrap.servers", "ec2-18-213-16-8.compute-1.amazonaws.com:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "ad0873818bc6c4a09a2e1dd273e04f6b9")
  props.put("auto.offset.reset", "earliest")


  println("===============")

  val consumer = new KafkaConsumer[String, String](props)

  println("===============")

  consumer.subscribe(java.util.Collections.singletonList(TOPIC))
  println("===============")

  while (true) {
    println("===============")

    val records = consumer.poll(Duration.ofSeconds(10))
    println(records.count())
    for (record <- records.asScala) {
      println(record.value())
    }
  }
}
