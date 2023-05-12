package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object KafkaStreamApplicationParsing2 extends App {

  val spark = SparkSession
    .builder
    .master("local[3]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

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

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("header", "true")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()
//  df.select(col("ride_id"),from_json(col("value").cast("string"), schema))
  val query1 = df.selectExpr("CAST(key AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("console")
    .outputMode("append")
    .start()



//  val query2 = df.foreach(r => println(r))
//    .format("console")
//    .outputMode("append")
//    .start()

    query1.awaitTermination()

}

