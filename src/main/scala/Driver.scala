import kafka.producer.CsvToKafkaProducer
import spark.streaming.KafkaStreamApplicationParsing

object Driver {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("No entrypoint provided!")
      System.exit(1)
    }

    val entrypoint = args(0)

    entrypoint match {
      case "Kafka" =>
        CsvToKafkaProducer.main(args)

      case "Spark" =>
        KafkaStreamApplicationParsing.main(args)

      case _ =>
        System.err.println("Unknown entrypoint: " + entrypoint)
        System.exit(1)
    }
  }


}
