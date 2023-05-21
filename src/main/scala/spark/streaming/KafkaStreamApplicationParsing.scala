package spark.streaming

import integration.kafka.KafkaOps
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import spark.streaming.data.RideCleaning
import spark.streaming.features.EndStations.percentageOfEndStations
import spark.streaming.features.{EndStations, RideOps, StartStations}
import spark.streaming.features.StartStations.percentageOfStartStations
import spark.streaming.integration.kibana.KibanaOps


object KafkaStreamApplicationParsing extends App {

  val sparkConf = new SparkConf().setAppName("SparkStreamingApp").setMaster("local[4]")

  val sparkContext = new SparkContext(sparkConf)
  sparkContext.setLogLevel("ERROR")

  // with checkpoint on EC2 instance:
  //val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))
  //sparkStreamingContext.checkpoint("hdfs://18.213.16.8:9000/testFolder") // TODO path anpassen
  // with S3
  sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "ASIATNLJ6XAJX4PR3NG3")
  sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "T6X3xbhoXeMtoXDMZaszLjlXRcMcguXXpwOXoPwb")
  sparkContext.hadoopConfiguration.set("fs.s3a.session.token", "FwoGZXIvYXdzEIr//////////wEaDLzsvPHcm5coLaiC1yLAAYYw8oLBXnWyWgDHENLAUKTYnId67b9jDl56B4HcdPxcVn9lxZs5A6BO+853jxzB6dQvqytnpR9vbvYRgzJGwV6Rdy78qwlh8VyWLhnkmOZWk7ZOrqtIjqRFUuUqfqNdXIqPEqRe6VwZTGrwHviC4bd7TYRoF7v82f9sjuw1dP/w6g5qejQhgxIePssysUvPoIZ8i8v1pMnKYobdYtMe+C74DiWnqMHJ3jMg1XUAJtMgSPY6hIJe9XU2h5NYpSM0dyjSoaejBjItveM0X4zY0fQut9fDIL3D0UaZXCM5yIDABejMLlab6M0AzD77BdBNP+e6MmHF")
  sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  sparkContext.setLogLevel("ERROR")
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(5))
  sparkStreamingContext.checkpoint("s3a://dis-checkpoint/checkpoint-directory")

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaOps.createKafkaStream(sparkStreamingContext)

  val cleanedStream = stream.filter(x => RideCleaning.isValidRide(x.value))
  val rideStream = cleanedStream.map(x => RideCleaning.parseToRide(x.value))

  /*rideStream.foreachRDD { rdd =>
    rdd.foreach { v => println(v)}
  }*/

  // test for start stations:
  StartStations.setTotal(0.0) // without regarding the first line - after testing back to 0.0
  percentageOfStartStations(rideStream)

  // test for end stations:
  //EndStations.setTotal(0.0) // without regarding the first line - after testing back to 0.0
  //percentageOfEndStations(rideStream)

  RideOps.allRides(rideStream)
  RideOps.rideDurationForBikeType(rideStream)
  //RideOps.rideDurationForUserType(rideStream)
  RideOps.averageNumberOfRides(rideStream)



  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()

}

