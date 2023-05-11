package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}


object KafkaStreamApplication2 extends App {

  val spark = SparkSession
    .builder
    .master("local[3]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  // Subscribe to 1 topic, with headers

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("header", "true")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest") // From starting
    .load()

  df.printSchema()

  val personStringDF = df.selectExpr("CAST(value AS STRING)")


  val schema = new StructType()
    .add("ride_id", StringType)
    .add("rideable_type", StringType)
    .add("started_at", StringType)
    .add("ended_at", StringType)
    .add("start_station_name", StringType)
    .add("start_station_id", StringType)
    .add("end_station_name", StringType)
    .add("end_station_id", StringType)
    .add("start_lat", StringType)
    .add("start_lng", StringType)
    .add("end_lat", StringType)
    .add("end_lng", StringType)
    .add("member_casual", StringType)

  val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
    .select("data.*")


  df.selectExpr("CAST(key AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

//  personDF.writeStream
//    .format("console")
//    .outputMode("append")
//    .start()
//    .awaitTermination()

}

