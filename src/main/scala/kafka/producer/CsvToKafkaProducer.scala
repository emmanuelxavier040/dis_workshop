package kafka.producer

import java.util.Properties
import java.io._

import org.apache.kafka.clients.producer._

object CsvToKafkaProducer {

  val topicName = "test"
  val bootstrapServers = "localhost:9092"

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // Create Kafka producer
    val producer = new KafkaProducer[String, String](props)

    // Read CSV file and send records to Kafka
    //val csvFilePath = "E:\\DIS\\kafka\\bin\\windows\\last_twelve_months\\202109-divvy-tripdata.csv"
    val csvFilePath = "/Users/lauraengist/cyclist_data_dis_workshop/202204-divvy-tripdata.csv"
    val file = new File(csvFilePath)
    val br = new BufferedReader(new FileReader(file))
    var line: String = null

    try {
      line = br.readLine()
      while (line != null) {

        println(line)
        val record = new ProducerRecord[String, String](topicName, line)

        producer.send(record)
        line = br.readLine()
        Thread.sleep(3)
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      producer.close()
      br.close()
    }
  }
}
