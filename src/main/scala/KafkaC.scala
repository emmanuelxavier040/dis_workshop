import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import scala.collection.JavaConverters._

object KafkaC extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "get.awesomedata.stream:9093")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "a8dd70c67162841eeb71cd18e213137a7")
  props.put("sasl.username", "public")
  props.put("sasl.password", "public")
  props.put("security.protocol", "SASL_PLAINTEXT")
  props.put("sasl.mechanism", "PLAIN")

  System.setProperty("java.security.auth.login.config", "D:\\MS\\Basel\\Course\\MS Sem1\\Distributed Information Systems\\Workshop\\Project\\SparkStreamApp\\resources\\jaas.conf")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(List("ratings").asJava)

  while (true) {
    val records = consumer.poll(Duration.ofSeconds(5))
    for (record <- records.asScala) {
      println(s"Received message: ${record.value}")
    }
  }
}
