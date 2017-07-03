package models

import cats.implicits._
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

sealed trait OffsetPosition

case object FromBeginning extends OffsetPosition
case object FromEnd extends OffsetPosition
case class AsPercentage(percentage: Int) extends OffsetPosition
case class AtOffset(offset: Long) extends OffsetPosition

object OffsetPosition {

  type OffsetPositionResult = Either[OffsetPositionError, Unit]
  final case class OffsetPositionError(reason: String)
  val NoTopicOrPartition = OffsetPositionError("topic or partition does not exist")
  type Consumer = KafkaConsumer[_, _]

  def apply(offsetStart: OffsetPosition, topic: String, partition: Int): Consumer => OffsetPositionResult = {

    def topicPartition =
      (c: Consumer) => c.assignment().asScala.find(tp => tp.topic() == topic && tp.partition() == partition)

    offsetStart match {
      case FromBeginning =>
        (c: Consumer) => topicPartition(c).fold[OffsetPositionResult](NoTopicOrPartition.asLeft)(tp => c.seekToBeginning(List(tp).asJavaCollection).asRight)
      case FromEnd =>
        (c: Consumer) => topicPartition(c).fold[OffsetPositionResult](NoTopicOrPartition.asLeft)(tp => c.seekToEnd(List(tp).asJavaCollection).asRight)
      case AsPercentage(percentage) =>
        ???
      case AtOffset(offset) =>
        (c: Consumer) => topicPartition(c).fold[OffsetPositionResult](NoTopicOrPartition.asLeft)(tp => c.seek(tp, offset).asRight)
    }
  }

}
