package kafka.producer

import java.util.Properties
import java.io._
import org.apache.kafka.clients.producer._

object CsvToKafkaProducer {

//  val topicName = "test"
  val topicName = "ride-topic"
//  val bootstrapServers = "localhost:9092"
  val bootstrapServers = "ec2-18-213-16-8.compute-1.amazonaws.com:9092"

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    processFilesForKafka(args(1), props)
//    processFilesForKafka("E:\\\\DIS\\\\kafka\\\\bin\\\\windows\\\\last_twelve_months", props)

  }

  def processFilesForKafka(csvDirectoryPath: String, props: Properties): Unit = {
    val producer = new KafkaProducer[String, String](props)
    val directory = new File(csvDirectoryPath)
    val files = directory.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.getPath).toList
    files.sorted.foreach(f => pushFileContentToKafka(f, producer))
    producer.close()
    }


  def pushFileContentToKafka(csvFilePath:String,  producer: KafkaProducer[String, String]): Unit = {
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
//        Thread.sleep(1)
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      br.close()
    }
  }
}



