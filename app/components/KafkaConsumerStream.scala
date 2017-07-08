package components

import actors.consumer.OffsetPosition
import models.Buffers.TopicPartitionBuffer
import monix.eval.MVar
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.reactivestreams.Subscriber

import scala.concurrent.duration.Duration

object KafkaConsumerStream {

  case class KafkaConsumerError(reason: String, error: Option[Throwable] = None)

  type ConfiguredKafkaConsumer[K, V] = Either[KafkaConsumerError, MVar[KafkaConsumer[K, V]]]

  def create[K, V](kafkaConsumer: ConfiguredKafkaConsumer[K, V])(implicit subscriber: Subscriber[_ >: ConsumerRecord[K, V]]): KafkaConsumerStream[K, V] = {

    ???
  }

}

trait KafkaConsumerStream[K, V] {
  import KafkaConsumerStream._

  val pollTimeout: Duration
  val kafkaConsumer: MVar[KafkaConsumer[K, V]]

  private var messageBuffers = Map[Int, TopicPartitionBuffer[K, V]]()

  def setTopic(topic: String): KafkaConsumerStream[K, V]

  def moveOffset(offsetPosition: OffsetPosition, topic: String, partition: Option[Int] = None): KafkaConsumerStream[K, V]

  def shutdown(): Either[KafkaConsumerError, Unit]

}