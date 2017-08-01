package components

import cats.implicits._
import models.ConsumerMessages._
import monix.eval.{ MVar, Task }
import org.apache.kafka.clients.consumer.{ ConsumerRecords, KafkaConsumer }

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object KafkaConsumerStream {

  type ConfiguredKafkaTopic = Either[KafkaConsumerError, String]
  type ConfiguredKafkaConsumer[K, V] = Either[KafkaConsumerError, KafkaConsumer[K, V]]
  type ConfiguredKafkaConsumerStream[K, V] = Either[KafkaConsumerError, KafkaConsumerStream[K, V]]
  type PolledConsumerRecords[K, V] = Either[KafkaConsumerError, ConsumerRecords[K, V]]

  def create[K, V](kafkaConsumer: ConfiguredKafkaConsumer[K, V]): ConfiguredKafkaConsumerStream[K, V] = {
    kafkaConsumer.flatMap { kc =>

      val _kafkaConsumerMVar = MVar(kc)
      val pollTimeout = 500 millis
      @volatile var stopped = false

      def pollingTask: Task[Either[KafkaConsumerError, MessagesPayload[K, V]]] =
        _kafkaConsumerMVar.take.flatMap(consumer => {
          val poll = if (!stopped) {
            try {
              MessagesPayload(consumer.poll(pollTimeout.toMillis)).asRight
            } catch {
              case t: Throwable => KafkaConsumerError("Error trying to poll Kafka", Some(t)).asLeft
            }
          } else {
            KafkaConsumerError("Stream stopped").asLeft
          }
          _kafkaConsumerMVar.put(consumer).map(_ => poll)
        })

      def stopTask: Task[Either[KafkaConsumerError, Unit]] = {
        if (stopped) {
          Task.now(KafkaConsumerError("Stream not running").asLeft)
        } else {
          val itsStopped = (stopped = true).asRight
          closeConsumer().map(_ => itsStopped)
        }
      }

      def closeConsumer(): Task[Unit] =
        _kafkaConsumerMVar.take.flatMap(consumer => {
          consumer.unsubscribe()
          consumer.close()
          _kafkaConsumerMVar.put(consumer)
        })

      val stream: KafkaConsumerStream[K, V] = new KafkaConsumerStream[K, V] {
        override val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]] = _kafkaConsumerMVar
        override def stop(): Task[Either[KafkaConsumerError, Unit]] = stopTask
        override def poll: Task[Either[KafkaConsumerError, MessagesPayload[K, V]]] = pollingTask
      }

      stream.asRight
    }
  }
}

sealed trait KafkaConsumerStream[K, V] {

  val kafkaConsumerMVar: MVar[KafkaConsumer[K, V]]

  def poll: Task[Either[KafkaConsumerError, MessagesPayload[K, V]]]
  def stop(): Task[Either[KafkaConsumerError, Unit]]

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
