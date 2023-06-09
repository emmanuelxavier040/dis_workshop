ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

libraryDependencies += "org.apache.kafka" %% "kafka" % "3.4.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0"

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "8.6.2"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreamApp"
  )
