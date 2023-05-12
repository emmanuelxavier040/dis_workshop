package test

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object StreamApplication extends App {

  val sparkConf = new SparkConf().setAppName("SparkStreamingApp").setMaster("local[2]")
  val sparkContext = new SparkContext(sparkConf)
  sparkContext.setLogLevel("ERROR")
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))

  val streamRDD = sparkStreamingContext.socketTextStream("127.0.0.1", 2222)
  val wordCounts = streamRDD.flatMap(line => line.split(" ").map(word => (word, 1))).
    reduceByKey(_+_)
  wordCounts.print()

  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()
}