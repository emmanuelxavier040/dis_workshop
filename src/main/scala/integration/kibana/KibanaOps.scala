package integration.kibana

import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.sparkRDDFunctions
import xyz.data.{Ride, RideCleaning}

object KibanaOps {

  def sendToELK(stream: DStream[Ride]): Unit = {
    val esNodes = "34.193.132.148:9200" // Comma-separated list of Elasticsearch nodes
    val esIndex = "test_index" // Target Elasticsearch index
    val esResource = s"$esIndex" // Resource path within the index

    // Set Elasticsearch configuration properties
    val esConfig = Map(
      "es.nodes" -> esNodes,
      "es.resource" -> esResource,
      "es.port" -> "9200",
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "l6tyvxoqQR2HsJLMjliK",
      "es.nodes.wan.only" -> "true"
    )

    val kibanaRides = stream.map(ride => RideCleaning.parseToRideForElastic(ride))

    kibanaRides.foreachRDD { rdd => {
        println("Here")
        rdd.foreach(ride => {println("At Ride : "); println(ride)})
        println(rdd.saveToEs(esConfig))
        println(rdd, "============WRITTEN=================")
      }
    }


  }
}
