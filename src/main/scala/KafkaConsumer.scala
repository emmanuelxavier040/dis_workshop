

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration

object KafkaConsumer extends App {

  import java.util.Properties

  val TOPIC = "book-ratings"

  val props = new Properties()
  props.put("bootstrap.servers", "get.awesomedata.stream:9093")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "ad0873818bc6c4a09a2e1dd273e04f6b9")
  props.put("sasl.username", "public")
  props.put("sasl.password", "public")
  props.put("security.protocol", "SASL_PLAINTEXT")
  props.put("sasl.mechanism", "PLAIN")
  System.setProperty("java.security.auth.login.config", "D:\\MS\\Basel\\Course\\MS Sem1\\Distributed Information Systems\\Workshop\\Project\\SparkStreamApp\\resources\\jaas.conf")

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
