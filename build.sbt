name := "off-topic"

version := "1.0"

lazy val `off_topic` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "Confluent" at "http://packages.confluent.io/maven"

scalaVersion := "2.12.3"

lazy val kafkaVersion = "0.10.2.1"
lazy val confluentVersion = "3.2.1"
lazy val avroVersion  = "1.8.2"

libraryDependencies ++= Seq(

  // Play
  guice,

  // Core
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,

  "org.apache.avro" % "avro" % avroVersion,
  "io.confluent" % "kafka-avro-serializer" % confluentVersion,

  // Misc
  "org.typelevel" %% "cats" % "0.9.0",
  "io.monix" %% "monix" % "2.3.0",

  // WebJars
  "org.webjars" % "react" % "15.6.1",
  "org.webjars.npm" % "react-dom" % "15.6.1",
  "org.webjars.bower" % "fetch" % "2.0.3",
  "org.webjars.npm" % "react-infinite" % "0.11.0",

  // Test
  "org.scalacheck" %% "scalacheck" % "1.13.5" % "test",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test"
)

scalacOptions in ThisBuild ++=  Seq(
  "-target:jvm-1.8",
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Xfatal-warnings"
)
