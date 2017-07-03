name := "off-topic"

version := "1.0"

lazy val `off_topic` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "Confluent" at "http://packages.confluent.io/maven"

scalaVersion := "2.12.2"

lazy val kafkaVersion = "0.10.2.1"
lazy val confluentVersion = "3.2.1"
lazy val avroVersion  = "1.8.2"

libraryDependencies ++= Seq(

  // Core
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,

  "org.apache.avro" % "avro" % avroVersion,
  "io.confluent" % "kafka-avro-serializer" % confluentVersion,

  // Misc
  "org.typelevel" %% "cats" % "0.9.0",
  "io.monix" %% "monix" % "2.3.0"
)
