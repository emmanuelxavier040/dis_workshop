package local_old


import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import java.time.Duration
import scala.collection.JavaConverters._

object KafkaConsumerRemote extends App {

  import java.util.Properties

  val brokerList = "ec2-18-213-16-8.compute-1.amazonaws.com:9092"
  val TOPIC = "my-new-topic"

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  // Create a Kafka consumer object
  val consumer = new KafkaConsumer[String, String](props)

  // Subscribe to the Kafka topic
  consumer.subscribe(Seq(TOPIC).asJava)

  // Read messages from the Kafka topic
  while (true) {
    val records = consumer.poll(Duration.ofSeconds(10))
    for (record <- records.asScala) {
      println("NEW VALUE:")
      println(record.value())
    }
  }
}
