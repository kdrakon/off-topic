package components

import actors.consumer.OffsetPosition
import cats.implicits._
import monix.eval.{MVar, Task}
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.reactivestreams.{Subscriber => ReactiveSubscriber}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object KafkaConsumerStream {

  case class KafkaConsumerError(reason: String, error: Option[Throwable] = None)

  type ConfiguredKafkaTopic = Either[KafkaConsumerError, String]
  type ConfiguredKafkaConsumer[K, V] = Either[KafkaConsumerError, KafkaConsumer[K, V]]
  type ConfiguredKafkaConsumerStream[K, V] = Either[KafkaConsumerError, KafkaConsumerStream[K, V]]
  type PolledConsumerRecords[K, V] = Either[KafkaConsumerError, ConsumerRecords[K, V]]

  def create[K, V](kafkaConsumer: ConfiguredKafkaConsumer[K, V])(implicit subscriber: ReactiveSubscriber[PolledConsumerRecords[K, V]], scheduler: Scheduler): ConfiguredKafkaConsumerStream[K, V] = {
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
      })

      val stream: KafkaConsumerStream[K, V] = new KafkaConsumerStream[K, V] {
        override val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]] = _kafkaConsumerMVar
        override def shutdown(): Either[KafkaConsumerError, Unit] = (streamCancelled = true).asRight
        override def start(): Either[KafkaConsumerError, Unit] = {
//          Task(Subscriber.fromReactiveSubscriber(subscriber, observable.subscribe())).runAsync
          observable.subscribe(cr => {
            println(cr.right.get.count())
            Task(Continue).runAsync
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

  def start(): Either[KafkaConsumerError, Unit]
  def shutdown(): Either[KafkaConsumerError, Unit]

  def setTopic(topic: ConfiguredKafkaTopic, newTopic: String): Task[ConfiguredKafkaConsumerStream[K, V]] = {
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

    task
  }

  def moveOffset(offsetPosition: OffsetPosition, topic: String, partition: Option[Int] = None): Task[ConfiguredKafkaConsumerStream[K, V]] = {
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

    task
  }
}
