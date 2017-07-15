package components

import actors.consumer.OffsetPosition
import akka.actor.ActorRef
import cats.implicits._
import models.ConsumerMessages._
import monix.eval.{ MVar, Task }
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.kafka.clients.consumer.{ ConsumerRecords, KafkaConsumer }

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object KafkaConsumerStream {

  type ConfiguredKafkaTopic = Either[KafkaConsumerError, String]
  type ConfiguredKafkaConsumer[K, V] = Either[KafkaConsumerError, KafkaConsumer[K, V]]
  type ConfiguredKafkaConsumerStream[K, V] = Either[KafkaConsumerError, KafkaConsumerStream[K, V]]
  type PolledConsumerRecords[K, V] = Either[KafkaConsumerError, ConsumerRecords[K, V]]

  def create[K, V](kafkaConsumer: ConfiguredKafkaConsumer[K, V])(implicit scheduler: Scheduler): ConfiguredKafkaConsumerStream[K, V] = {
    kafkaConsumer.flatMap { kc =>

      val _kafkaConsumerMVar = MVar(kc)
      val pollTimeout = 5 seconds
      var streamCancelled = false

      val observable = Observable.fromIterator(new Iterator[PolledConsumerRecords[K, V]] {
        override def hasNext: Boolean = !streamCancelled

        override def next(): PolledConsumerRecords[K, V] = {
          val task = _kafkaConsumerMVar.take.flatMap(consumer => {
            val records = consumer.poll(pollTimeout.toMillis)
            _kafkaConsumerMVar.put(consumer).map(_ => records)
          })

          task.runAsync.value match {
            case Some(Success(consumerRecords)) => consumerRecords.asRight
            case Some(Failure(t)) => KafkaConsumerError("Failure polling for ConsumerRecords", Some(t)).asLeft
            case None => KafkaConsumerError("Unknown error encountered polling for ConsumerRecords").asLeft
          }
        }
      }).filter(_.fold[Boolean](_ => true, !_.isEmpty)) // remove only empty results

      val stream: KafkaConsumerStream[K, V] = new KafkaConsumerStream[K, V] {
        override val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]] = _kafkaConsumerMVar
        override def shutdown(): Either[KafkaConsumerError, Unit] = (streamCancelled = true).asRight
        override def start(subscriber: ActorRef): Either[KafkaConsumerError, Unit] = {
          observable.foreach({
            case Right(records) =>
              subscriber ! MessagesPayload(records)
            case Left(err) =>
              subscriber ! err
              streamCancelled = true
          })
          ().asRight
        }
      }

      stream.asRight
    }
  }
}

sealed trait KafkaConsumerStream[K, V] {
  import KafkaConsumerStream._

  val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]]
  private var kafkaTopic: ConfiguredKafkaTopic = KafkaConsumerError("Topic not yet set").asLeft

  def start(subscriber: ActorRef): Either[KafkaConsumerError, Unit]
  def shutdown(): Either[KafkaConsumerError, Unit]

  def setTopic(newTopic: String): Task[ConfiguredKafkaTopic] = {
    def error[T <: Throwable](t: T) = KafkaConsumerError(s"Failed to subscribe to topic $newTopic", Some(t)).asLeft
    kafkaConsumerMVar.take.flatMap(consumer => {

      def subscribe(): ConfiguredKafkaTopic = {
        try {
          consumer.unsubscribe()
          consumer.subscribe(List(newTopic).asJavaCollection)
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

  def moveOffset(offsetPosition: OffsetPosition, topic: String, partition: Partition = AllPartitions): Task[Either[KafkaConsumerError, List[Unit]]] = {
    kafkaConsumerMVar.take.flatMap(consumer => {
      val partitions = partition match {
        case AllPartitions => consumer.assignment().asScala.map(_.partition()).toSeq
        case APartition(p) => Seq(p)
      }

      val offset = partitions.map(p => {
        OffsetPosition(offsetPosition, topic, p)(consumer).leftMap(offsetErr => KafkaConsumerError(offsetErr.reason))
      }).toList.sequenceU

      kafkaConsumerMVar.put(consumer).map(_ => offset)

    }).onErrorHandle(t => KafkaConsumerError("Failed to move offset of consumer", Some(t)).asLeft)
  }
}
