package components

import actors.consumer.OffsetPosition
import cats.data.EitherT
import cats.implicits._
import models.Buffers.TopicPartitionBuffer
import monix.eval.{MVar, Task}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object KafkaConsumerStream {

  case class KafkaConsumerError(reason: String, error: Option[Throwable] = None)

  type ConfiguredKafkaTopic = Either[KafkaConsumerError, String]
  type ConfiguredKafkaConsumer[K, V] = Either[KafkaConsumerError, KafkaConsumer[K, V]]
  type ConfiguredKafkaConsumerStream[K, V] = EitherT[Task, KafkaConsumerError, KafkaConsumerStream[K, V]]

  def create[K, V](kafkaConsumer: ConfiguredKafkaConsumer[K, V])(implicit subscriber: Subscriber[Task[ConsumerRecords[K, V]]]): ConfiguredKafkaConsumerStream[K, V] = {
    val stream = kafkaConsumer.flatMap { kc =>

      val _kafkaConsumerMVar = MVar(kc)
      val pollTimeout = 5 seconds
      var streamCancelled = false

      val observable = Observable.fromIterator(new Iterator[Task[ConsumerRecords[K, V]]] {
        override def hasNext: Boolean = !streamCancelled
        override def next(): Task[ConsumerRecords[K, V]] = {
          _kafkaConsumerMVar.take.flatMap(consumer => {
            val records = consumer.poll(pollTimeout.toMillis)
            _kafkaConsumerMVar.put(consumer).map(_ => records)
          })
        }
      })

      val cancelable = observable.subscribe(subscriber)

      val stream: KafkaConsumerStream[K, V] = new KafkaConsumerStream[K, V] {
        override val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]] = _kafkaConsumerMVar
        override def shutdown(): Either[KafkaConsumerError, Unit] = {
          streamCancelled = true
          cancelable.cancel().asRight
        }
      }

      stream.asRight
    }

    EitherT(Task.now(stream))
  }
}

trait KafkaConsumerStream[K, V] {
  import KafkaConsumerStream._

  private[KafkaConsumerStream] val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]]
  private[KafkaConsumerStream] var messageBuffers = Map[Int, TopicPartitionBuffer[K, V]]()

  def shutdown(): Either[KafkaConsumerError, Unit]

  def setTopic(topic: ConfiguredKafkaTopic, newTopic: String): ConfiguredKafkaConsumerStream[K, V] = {
    val stream = this.asRight
    val task: Task[Either[KafkaConsumerError, KafkaConsumerStream[K, V]]] =
      kafkaConsumerMVar.take.flatMap(consumer => {

        def subscribe(): Unit = {
          consumer.unsubscribe()
          consumer.subscribe(List(newTopic).asJavaCollection)
        }

        topic match {
          case Left(_) =>
            subscribe()
            topic.asRight
          case Right(existing) =>
            if (existing != newTopic) {
              subscribe()
              topic.asRight
            } else {
              existing.asRight
            }
        }

        kafkaConsumerMVar.put(consumer)
      })
        .onErrorHandle(t => KafkaConsumerError("Failed to set topic on consumer", Some(t)).asLeft)
        .map(_ => stream)

    EitherT(task)
  }

  def moveOffset(offsetPosition: OffsetPosition, topic: String, partition: Option[Int] = None): ConfiguredKafkaConsumerStream[K, V] = {
    val stream = this.asRight
    val task: Task[Either[KafkaConsumerError, KafkaConsumerStream[K, V]]] =
      kafkaConsumerMVar.take.flatMap(consumer => {

        partition.fold(consumer.assignment().asScala.map(_.partition()).toSeq)(Seq(_)).map(p => {
          OffsetPosition(offsetPosition, topic, p)(consumer)
            .leftMap(offsetErr => KafkaConsumerError(offsetErr.reason))
        })

        kafkaConsumerMVar.put(consumer)
      })
        .onErrorHandle(t => KafkaConsumerError("Failed to move offset of consumer", Some(t)).asLeft)
        .map(_ => stream)

    EitherT(task)
  }
}
