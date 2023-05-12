package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object KafkaStreamApplication2 extends App {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession
    .builder
    .master("local[3]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  // Subscribe to 1 topic, with headers

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "ec2-18-213-16-8.compute-1.amazonaws.com:9092")
    .option("header", "true")
    .option("subscribe", "my-new-topic")
    .option("kafka.group.id","ad0873818bc6c4a09a2e1dd273e04f6b9")

    .option("startingOffsets", "earliest") // From starting
    .load()

  val rides  = df.select("key")
  rides.writeStream
    .format("console")
        .outputMode("append")
        .start()
        .awaitTermination()
//    df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
//    .writeStream
//    .format("console")
//      .outputMode("append")
//      .start()
//      .awaitTermination()


}

