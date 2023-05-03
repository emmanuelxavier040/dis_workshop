ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"

//libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10_2.12" % sparkVersion
libraryDependencies += "org.apache.kafka" %% "kafka" % "3.4.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreamApp"
  )
