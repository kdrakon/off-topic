package models

import java.util.Properties

import actors.consumer.ConsumerActor.{AvroConsumer, ConsumerType, StringConsumer}
import actors.consumer.{AtOffset, FromBeginning, FromEnd, OffsetPosition}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import play.api.libs.json._

import scala.collection.JavaConverters._

object ConsumerMessages {

  sealed trait ConsumerMessage

  case class KafkaConsumerError(reason: String, error: Option[Throwable] = None) extends ConsumerMessage

  case class ConsumerConfig(consumerId: String, topic: String, props: Properties, consumerType: ConsumerType)
  case class CreateConsumer(conf: ConsumerConfig) extends ConsumerMessage

  case object ShutdownConsumer extends ConsumerMessage

  case class StartConsumer(offsetStart: OffsetPosition) extends ConsumerMessage

  sealed trait Partition
  case object AllPartitions extends Partition
  case class APartition(number: Int) extends Partition
  case class MoveOffset(partition: Partition, offsetPosition: OffsetPosition) extends ConsumerMessage

  case class MessagesPayload[K,V](payload: ConsumerRecords[K, V]) extends ConsumerMessage

  object jsonformats {

    implicit val ThrowableFormat: Format[java.lang.Throwable] = Format(
      Reads[java.lang.Throwable]({
        case JsObject(map) => map.get("throwable").fold[JsResult[java.lang.Throwable]](JsError("Not a java.lang.Throwable"))(reason => JsSuccess(new Throwable(reason.as[String])))
        case _ => JsError("Not a java.lang.Throwable")
      }),
      Writes[java.lang.Throwable](t => JsObject(Map("throwable" -> JsString(t.getMessage))))
    )

    implicit val OptionalThrowableFormat: Format[Option[java.lang.Throwable]] = Format(
      Reads[Option[java.lang.Throwable]]({
        case JsNull => JsSuccess(None)
        case js => ThrowableFormat.reads(js).map(t => Some(t))
      }),
      Writes[Option[java.lang.Throwable]](_ => JsNull)
    )

    implicit val ConsumerTypeFormat = Format(
      Reads[ConsumerType]({
        case JsString("StringConsumer") => JsSuccess(StringConsumer)
        case JsString("AvroConsumer") => JsSuccess(AvroConsumer)
        case _ => JsError("Not a ConsumerType")
      }),
      Writes[ConsumerType]({
        case StringConsumer => JsString("StringConsumer")
        case AvroConsumer => JsString("AvroConsumer")
      })
    )

    implicit val JavaPropertiesFormat = Format(
      Reads[java.util.Properties] {
        case JsObject(props) =>
          val javaProps = new  java.util.Properties()
          props.foreach {
            case (k, v) => javaProps.setProperty(k, v.as[String])
          }
          JsSuccess(javaProps)
        case _ => JsError("Not a java.util.Properties object")
      },
      Writes[java.util.Properties](p => {
        JsObject(p.propertyNames().asScala.map(_.toString).map(name => name -> JsString(p.getProperty(name))).toMap[String, JsValue])
      })
    )

    implicit val ConsumerConfigFormat: Format[ConsumerConfig] = Json.format[ConsumerConfig]
    implicit val OffsetPosition: Format[OffsetPosition] = Format(
      Reads[OffsetPosition]({
        case JsString("FromBeginning") => JsSuccess(FromBeginning)
        case JsString("FromEnd") => JsSuccess(FromEnd)
        case atOffset: JsNumber => JsSuccess(AtOffset(atOffset.as[Long]))
        case _ => JsError("Invalid OffsetPosition")
      }),
      Writes[OffsetPosition]({
        case FromBeginning => JsString("FromBeginning")
        case FromEnd => JsString("FromEnd")
        case AtOffset(offset) => JsNumber(offset)
      })
    )

    implicit val PartitionFormat: Format[Partition] = Format(
      Reads[Partition] {
        case JsString("AllPartitions") => JsSuccess(AllPartitions)
        case JsNumber(n) => JsSuccess(APartition(n.toInt))
        case _ => JsError("Not a valid Partition")
      },
      Writes[Partition] {
        case AllPartitions => JsString("AllPartitions")
        case APartition(n) => JsNumber(n)
      }
    )

    implicit val TopicPartitionFormat: Format[TopicPartition] = Format(
      Reads[TopicPartition]({
        case JsString(topicPartition) =>
          val split = topicPartition.split("][")
          try {
            JsSuccess(new TopicPartition(split.head, split.tail.reverse.head.toInt))
          } catch {
            case t: Throwable => JsError(t.getMessage)
          }
        case _ => JsError("Invalid TopicPartition")
      }),
      Writes[TopicPartition](tp => JsString(s"${tp.topic()}][${tp.partition()}"))
    )

    implicit val ConsumerRecordWrites: Writes[ConsumerRecord[_, _]] = Writes { cr =>
      JsObject {
        Map(
          "key" -> JsString(cr.key().toString),
          "value" -> JsString(cr.value().toString),
          "offset" -> JsNumber(cr.offset())
        )
      }
    }

    implicit val KafkaConsumerErrorFormat: Format[KafkaConsumerError] = Json.format[KafkaConsumerError]
    implicit val CreateConsumerFormat: Format[CreateConsumer] = Json.format[CreateConsumer]
    implicit val StartConsumerFormat: Format[StartConsumer] = Json.format[StartConsumer]
    implicit val MoveOffsetFormat: Format[MoveOffset] = Json.format[MoveOffset]

    implicit val MessagesPayloadWrites: Writes[MessagesPayload[_, _]] = Writes { mp =>
      JsObject {
        Seq(
          "payload" ->
            JsObject {
              mp.payload.partitions().asScala.map(topicPartition => {
                val jsArray = JsArray(mp.payload.records(topicPartition).asScala.sortBy(_.offset()).map(ConsumerRecordWrites.writes))
                TopicPartitionFormat.writes(topicPartition).as[String] -> jsArray
              }).toMap
            }
        )
      }
    }

    implicit val ConsumerMessageFormat: Format[ConsumerMessage] = Format(
      Reads[ConsumerMessage]({
        case o: JsObject if o ? "KafkaConsumerError" => KafkaConsumerErrorFormat.reads(UnWrapper("KafkaConsumerError", o))
        case o: JsObject if o ? "CreateConsumer" => CreateConsumerFormat.reads(UnWrapper("CreateConsumer", o))
        case o: JsObject if o ? "ShutdownConsumer" => JsSuccess(ShutdownConsumer)
        case o: JsObject if o ? "StartConsumer" => StartConsumerFormat.reads(UnWrapper("StartConsumer", o))
        case o: JsObject if o ? "MoveOffset" => MoveOffsetFormat.reads(UnWrapper("MoveOffset", o))
        case o: JsObject if o ? "MessagesPayload" => JsError("Cannot read MessagesPayload type")
      }),

      Writes[ConsumerMessage] {
        case m: KafkaConsumerError => Wrapper("KafkaConsumerError", KafkaConsumerErrorFormat.writes(m))
        case m: CreateConsumer => Wrapper("CreateConsumer", CreateConsumerFormat.writes(m))
        case ShutdownConsumer => Wrapper("ShutdownConsumer", JsObject.empty)
        case m: StartConsumer => Wrapper("StartConsumer", StartConsumerFormat.writes(m))
        case m: MoveOffset => Wrapper("MoveOffset", MoveOffsetFormat.writes(m))
        case m: MessagesPayload[_, _] => Wrapper("MessagesPayload", MessagesPayloadWrites.writes(m))
      }
    )

    private[this] def Wrapper(name: String, message: JsValue) = message.as[JsObject] ++ JsObject(Map("ConsumerMessage" -> JsString(name)))
    private[this] def UnWrapper(name: String, message: JsObject) = JsObject(message.value.filterKeys(_ != "ConsumerMessage"))
    private implicit class WrapperImplicit(js: JsObject) {
      def ?(name: String): Boolean = js.value.get("ConsumerMessage").fold(false)(n => n == JsString(name))
    }

  }

}