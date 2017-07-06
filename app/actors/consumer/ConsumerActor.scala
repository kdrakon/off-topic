package actors.consumer

import java.util.Properties

import actors.consumer.ConsumerActor._
import actors.consumer.ConsumerSupervisorActor.ExistingConsumer
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, SupervisorStrategy}
import cats.implicits._
import models.Buffers.TopicPartitionBuffer
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object ConsumerActor {
  case class ConsumerConfig(id: String, props: Properties)
  case class CreateConsumer(conf: ConsumerConfig)
  case class ShutdownConsumer(id: String)
  case class StartConsumer(topic: String, partition: Int, offsetStart: OffsetPosition)
  case class MoveOffset(topic: String, partition: Int, offsetPosition: OffsetPosition)

  case object PollMessages
  case class GetMessages(offset: Long, partition: Int, maxMessages: Int)
  case class MessagesPayload[K,V](payload: Seq[ConsumerRecord[K, V]])
  case object EndOfMessageBuffer

  implicit class MessageBufferImplicits[K, V](buffers: Map[Int, TopicPartitionBuffer[K, V]]) {
    def totalBufferSize: Int = buffers.foldLeft(0) {
      case (total, (_, buffer)) => total + buffer.size
    }
  }

  implicit class SeqConsumerGroupImplicits[K, V](seq: Seq[ConsumerRecord[K, V]]){
    def toOffsetMap: Map[Long, ConsumerRecord[K, V]] = seq.map(cr => (cr.offset(), cr)).toMap
  }
}

trait ConsumerActor[K, V] extends Actor {

  import ConsumerActor._

  case class KafkaConsumerError(reason: String, error: Option[Throwable] = None)

  type ConfiguredKafkaConsumer = Either[KafkaConsumerError, KafkaConsumer[K, V]]
  type UpdatedOffsets = Task[Either[KafkaConsumerError, OffsetMap]]
  type OffsetMap = Map[Int, Long]

  implicit val scheduler = Scheduler(context.dispatcher)

  private var kafkaConsumer: ConfiguredKafkaConsumer = KafkaConsumerError("Consumer not yet created").asLeft
  private var polling: Boolean = false
  private val pollInterval = 500 millis
  private val bufferSize = 1000
  private var messageBuffers = Map[Int, TopicPartitionBuffer[K, V]]()
  private def maxMessageBuffersSize = bufferSize * messageBuffers.keys.size
  private var offsetMap = Map[Int, Long]()

  override def receive: Receive = {

    case CreateConsumer(conf) => kafkaConsumer = kafkaConsumer.fold(_ => createConsumer(conf), c => c.asRight)
    case ShutdownConsumer(_) => shutdownConsumer()
    case _: StartConsumer => startConsumer(_)
    case m: MoveOffset => moveOffset(m.offsetPosition, m.topic, m.partition)

    case GetMessages(offset, partition, maxMessages) =>
      messageBuffers.get(partition).foreach(buffer => {
        sender ! MessagesPayload(buffer.take(offset, maxMessages))
      })

    case PollMessages if polling && messageBuffers.totalBufferSize < maxMessageBuffersSize =>
      pollMessages.map(_.map(offsetMap ++= _))
        .doOnFinish(_ => Task(context.system.scheduler.scheduleOnce(pollInterval, self, PollMessages))).runAsync
  }

  def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer

  private def shutdownConsumer(): Unit = {
    kafkaConsumer.foreach(con => {
      con.unsubscribe()
      con.close()
      polling = false
    })
    kafkaConsumer = KafkaConsumerError("Consumer was shutdown and no longer usable").asLeft
    self ! PoisonPill
  }

  private def startConsumer(start: StartConsumer): Either[KafkaConsumerError, Unit] = {
    kafkaConsumer.flatMap(con => {
      con.unsubscribe()
      con.subscribe(List(start.topic).asJavaCollection)
      messageBuffers = newMessageBuffer(con.partitionsFor(start.topic).asScala)
      offsetMap = Map()
      polling = true
      (self ! PollMessages).asRight
    }).flatMap(_ => moveOffset(start.offsetStart, start.topic, start.partition))
  }

  private def moveOffset(offsetPosition: OffsetPosition, topic: String, partition: Int): Either[KafkaConsumerError, Unit] = {
    kafkaConsumer.map(con => {
      OffsetPosition(offsetPosition, topic, partition)(con)
        .leftMap(offsetErr => KafkaConsumerError(offsetErr.reason))
    })
  }

  private def pollMessages: UpdatedOffsets = Task {
    kafkaConsumer.map(con => {
      con.poll((5 seconds).toMillis).iterator().asScala.toSeq.groupBy(_.partition()).map({
        case (partition, consumerRecords) =>
          messageBuffers.get(partition).map(buffer => {
            val crMap = consumerRecords.toOffsetMap
            buffer.add(crMap)
            (partition, crMap.keySet.max)
          })
      }).toMap[Int, Long]
    })
  }

  private def newMessageBuffer(partitions: Seq[PartitionInfo]) : Map[Int, TopicPartitionBuffer[K, V]] = {
    partitions.map(p => (p.partition(), TopicPartitionBuffer[K, V](bufferSize))).toMap
  }
}

class StringConsumerActor extends ConsumerActor[String, String] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer = {
    try {
      val consumer = new KafkaConsumer[String, String](conf.props, new StringDeserializer(), new StringDeserializer())
      consumer.asRight
    } catch {
      case t: Throwable => KafkaConsumerError("Failed to create consumer", Some(t)).asLeft
    }
  }
}

class AvroConsumerActor extends ConsumerActor[Any, GenericRecord] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer = {
    ???
  }
}

class ConsumerSupervisorActor extends Actor {

  var consumers: Map[String, ExistingConsumer] = Map()

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: Throwable => Stop
  }

  override def receive: Receive = ???
}

object ConsumerSupervisorActor {

  case class ExistingConsumer(consumerActor: ActorRef)


}
