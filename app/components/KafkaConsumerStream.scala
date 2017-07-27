package components

import actors.consumer.OffsetPosition
import akka.actor.ActorRef
import cats.implicits._
import models.ConsumerMessages._
import monix.eval.{ MVar, Task }
import monix.execution.{ CancelableFuture, Scheduler }
import org.apache.kafka.clients.consumer.{ ConsumerRecords, KafkaConsumer }
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object KafkaConsumerStream {

  type ConfiguredKafkaTopic = Either[KafkaConsumerError, String]
  type ConfiguredKafkaConsumer[K, V] = Either[KafkaConsumerError, KafkaConsumer[K, V]]
  type ConfiguredKafkaConsumerStream[K, V] = Either[KafkaConsumerError, KafkaConsumerStream[K, V]]
  type PolledConsumerRecords[K, V] = Either[KafkaConsumerError, ConsumerRecords[K, V]]

  def create[K, V](kafkaConsumer: ConfiguredKafkaConsumer[K, V])(implicit scheduler: Scheduler): ConfiguredKafkaConsumerStream[K, V] = {
    kafkaConsumer.flatMap { kc =>

      val _kafkaConsumerMVar = MVar(kc)
      val pollTimeout = 500 millis
      @volatile var stopped = false

      def closeConsumer(): Task[Unit] =
        _kafkaConsumerMVar.take.flatMap(consumer => {
          consumer.unsubscribe()
          consumer.close()
          _kafkaConsumerMVar.put(consumer)
        })

      def pollingTask(subscriber: ActorRef): Task[Unit] =
        _kafkaConsumerMVar.take.flatMap(consumer => {
          if (!stopped) subscriber ! MessagesPayload(consumer.poll(pollTimeout.toMillis))
          _kafkaConsumerMVar.put(consumer)
        }).doOnFinish({
          case None => if (!stopped) pollingTask(subscriber) else Task.unit
          case Some(e) => Task(subscriber ! KafkaConsumerError("Polling of Kafka has failed", Some(e)))
        })

      val stream: KafkaConsumerStream[K, V] = new KafkaConsumerStream[K, V] {
        override val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]] = _kafkaConsumerMVar
        override def stop(): Either[KafkaConsumerError, Unit] = {
          if (stopped) {
            KafkaConsumerError("Stream not running").asLeft
          } else {
            closeConsumer().runAsync
            (stopped = true).asRight
          }
        }
        override def start(subscriber: ActorRef): CancelableFuture[Unit] = pollingTask(subscriber).delayExecution(pollTimeout).runAsync
      }

      stream.asRight
    }
  }
}

sealed trait KafkaConsumerStream[K, V] {
  import KafkaConsumerStream._

  val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]]
  private var kafkaTopic: ConfiguredKafkaTopic = KafkaConsumerError("Topic not yet set").asLeft

  def start(subscriber: ActorRef): CancelableFuture[Unit]
  def stop(): Either[KafkaConsumerError, Unit]

  def setTopic(newTopic: String): Task[ConfiguredKafkaTopic] = {
    def error[T <: Throwable](t: T) = KafkaConsumerError(s"Failed to subscribe to topic $newTopic", Some(t)).asLeft
    kafkaConsumerMVar.take.flatMap(consumer => {

      def subscribe(): ConfiguredKafkaTopic = {
        try {
          consumer.unsubscribe()
          val partitions = consumer.partitionsFor(newTopic).asScala.map(p => new TopicPartition(p.topic(), p.partition())).asJavaCollection
          consumer.assign(partitions)
          newTopic.asRight
        } catch {
          case t: Throwable => error(t)
        }
      }

      kafkaTopic = kafkaTopic match {
        case Left(_) =>
          subscribe()
        case Right(existing) =>
          if (existing != newTopic) {
            subscribe()
          } else {
            existing.asRight
          }
      }

      kafkaConsumerMVar.put(consumer).map(_ => kafkaTopic)

    }).onErrorHandle(error)
  }

  def moveOffset(offsetPosition: OffsetPosition, topic: String, partition: Partition = AllPartitions): Task[Either[KafkaConsumerError, Unit]] = {
    kafkaConsumerMVar.take.flatMap(consumer => {
      val partitions = partition match {
        case AllPartitions => consumer.partitionsFor(topic).asScala.map(_.partition())
        case APartition(p) => Seq(p)
      }

      val offset = partitions.map(p => {
        OffsetPosition(offsetPosition, topic, p)(consumer).leftMap(offsetErr => KafkaConsumerError(offsetErr.reason))
      }).toList.sequenceU

      kafkaConsumerMVar.put(consumer).map(_ => offset.map(_ => ()))

    }).onErrorHandle(t => KafkaConsumerError("Failed to move offset of consumer", Some(t)).asLeft)
  }
}
