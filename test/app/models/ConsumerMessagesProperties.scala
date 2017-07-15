package app.models

import actors.consumer.ConsumerActor.StringConsumer
import actors.consumer.{ AtOffset, FromBeginning, FromEnd }
import models.ConsumerMessages
import models.ConsumerMessages._
import org.apache.kafka.clients.consumer.{ ConsumerRecord, ConsumerRecords }
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Prop._
import org.scalacheck.{ Gen, Properties }
import play.api.libs.json.{ JsArray, JsObject }

import scala.collection.JavaConverters._

class ConsumerMessagesProperties extends Properties(ConsumerMessages.getClass.getName) {

  import ConsumerMessages.jsonformats._

  def writesAndReadsMessage[T <: ConsumerMessage](m: T, equality: (T, T) => Boolean = (t1: T, t2: T) => t1 == t2) = {
    equality(ConsumerMessageFormat.reads(ConsumerMessageFormat.writes(m)).get.asInstanceOf[T], m)
  }

  val KafkaConsumerErrorFormat = forAll { (reason: String) =>
    writesAndReadsMessage[KafkaConsumerError](KafkaConsumerError(reason))
    writesAndReadsMessage[KafkaConsumerError](KafkaConsumerError(reason, Some(new RuntimeException("oops"))), (t1, t2) => {
      t1.reason == t2.reason && t1.error.get.getMessage == t2.error.get.getMessage
    })
  }

  val CreateConsumerFormat = forAll { (consumerId: String, properties: Map[String, String], topic: String) =>
    val props = new java.util.Properties()
    properties.foreach({
      case (k, v) => props.setProperty(k, v)
    })
    assert(JavaPropertiesFormat.reads(JavaPropertiesFormat.writes(props)).get == props)
    writesAndReadsMessage(CreateConsumer(ConsumerConfig(consumerId, topic, props, StringConsumer)))
  }

  val ShutdownConsumerFormat = all(writesAndReadsMessage(ShutdownConsumer))

  val StartConsumerFormat = forAll { (offset: Long) =>
    writesAndReadsMessage(StartConsumer(FromBeginning))
    writesAndReadsMessage(StartConsumer(FromEnd))
    writesAndReadsMessage(StartConsumer(AtOffset(offset)))
  }

  val MoveOffsetFormat = forAll { (partition: Int, offset: Long) =>
    writesAndReadsMessage(MoveOffset(AllPartitions, FromBeginning))
    writesAndReadsMessage(MoveOffset(APartition(partition), FromBeginning))
    writesAndReadsMessage(MoveOffset(AllPartitions, FromEnd))
    writesAndReadsMessage(MoveOffset(APartition(partition), FromEnd))
    writesAndReadsMessage(MoveOffset(AllPartitions, AtOffset(offset)))
    writesAndReadsMessage(MoveOffset(APartition(partition), AtOffset(offset)))
  }

  val MessagesPayloadFormat = forAll { (topics: List[String], partitions: List[Int], keys: List[String]) =>
    val offsetGen = Gen.chooseNum(1L, 10000L)
    val valueGen = Gen.alphaStr
    val keyValues = keys.map(k => k -> valueGen.sample.get).toMap
    val topicPartitions = topics.flatMap(t => partitions.map(p => new TopicPartition(t, p)))
    val cr = topicPartitions.map(tp => {
      tp -> keyValues.map(kv => new ConsumerRecord[String, String](tp.topic(), tp.partition(), offsetGen.sample.get, kv._1, kv._2)).toList.asJava
    }).toMap
    val mp = MessagesPayload(new ConsumerRecords[String, String](cr.asJava))

    val json = ConsumerMessageFormat.writes(mp).as[JsObject]
    val payloadObject = json.value("payload").as[JsObject]
    val shouldNotBeAbleToRead = all(ConsumerMessageFormat.reads(json).isError) :| "Can not use Reads on MessagePayload"
    val topicPartitionsAreKeys = all(topicPartitions.forall(tp => payloadObject.value.keySet.contains(TopicPartitionFormat.writes(tp).as[String]))) :| "All TopicPartitions are used as keys"

    val hasAllConsumerRecords = all {
      payloadObject.value.keySet.forall(topicPartition => {
        val jsonKeyValues = payloadObject.value(topicPartition).as[JsArray].value.map(_.as[JsObject])
        jsonKeyValues.forall(obj => {
          keyValues(obj.value("key").as[String]) == obj.value("value").as[String] && obj.value("offset").as[Long] >= 1L && obj.value("offset").as[Long] <= 10000L
        })
      })
    } :| "All ConsumerRecords mapped to JSON"

    all(shouldNotBeAbleToRead, topicPartitionsAreKeys, hasAllConsumerRecords)
  }

  property("ConsumerMessageFormat") = all(
    KafkaConsumerErrorFormat :| "KafKaConsumerError",
    CreateConsumerFormat :| "CreateConsumer",
    ShutdownConsumerFormat :| "ShutdownConsumer",
    StartConsumerFormat :| "StartConsumer",
    MoveOffsetFormat :| "MoveOffsetFormat",
    MessagesPayloadFormat :| "MessagesPayloadWrites"
  )
}
