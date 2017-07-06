package actors.consumer

import java.util.{Properties, UUID}

import actors.consumer.ConsumerActor._
import actors.consumer.ConsumerSupervisorActor.ExistingConsumer
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy}
import cats.implicits._
import models.Buffers.TopicPartitionBuffer
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object ConsumerActor {

  sealed trait ConsumerType
  case object StringConsumer extends ConsumerType
  case object AvroConsumer extends ConsumerType

  case class ConsumerConfig(consumerId: String, props: Properties, consumerType: ConsumerType)
  case class CreateConsumer(conf: ConsumerConfig)
  case class ShutdownConsumer(consumerId: String)
  case class StartConsumer(topic: String, offsetStart: OffsetPosition)
  case class MoveOffset(topic: String, partition: Int, offsetPosition: OffsetPosition)

  case object PollMessages
  case class GetMessages(offset: Long, partition: Int, maxMessages: Int)
  case class MessagesPayload[K,V](payload: Seq[ConsumerRecord[K, V]])
  case object EndOfMessageBuffer

  def genConsumerId: String = UUID.randomUUID().toString

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
  import ConsumerActor._, ConsumerSupervisorActor._

  case class KafkaConsumerError(reason: String, error: Option[Throwable] = None)

  type ConfiguredKafkaConsumer = Either[KafkaConsumerError, KafkaConsumer[K, V]]
  type ConfiguredKafkaTopic = Either[KafkaConsumerError, String]
  type UpdatedOffsets = Task[Either[KafkaConsumerError, OffsetMap]]
  type OffsetMap = Map[Int, Long]

  implicit val scheduler = Scheduler(context.dispatcher)

  private var kafkaConsumer: ConfiguredKafkaConsumer = KafkaConsumerError("Consumer not yet created").asLeft
  private var kafkaTopic: ConfiguredKafkaTopic = KafkaConsumerError("Topic not yet set").asLeft
  private var polling: Boolean = false
  private val pollInterval = 500 millis
  private val bufferSize = 1000
  private var messageBuffers = Map[Int, TopicPartitionBuffer[K, V]]()
  private def maxMessageBuffersSize = bufferSize * messageBuffers.keys.size
  private var offsetMap = Map[Int, Long]()

  override def receive: Receive = {

    case CreateConsumer(conf) =>
      kafkaConsumer = kafkaConsumer.fold(_ => createConsumer(conf), c => c.asRight)
      sender ! ExistingConsumer(self)

    case ShutdownConsumer(_) => shutdownConsumer()
    case _: StartConsumer => startConsumer(_)
    case m: MoveOffset => moveOffset(m.offsetPosition, m.topic, Some(m.partition))

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
      kafkaTopic = subscribeToTopic(start.topic)
      messageBuffers = newMessageBuffer(con.partitionsFor(start.topic).asScala)
      offsetMap = Map()
      polling = true
      (self ! PollMessages).asRight
    }).flatMap(_ => moveOffset(start.offsetStart, start.topic))
  }

  private def subscribeToTopic(topic: String): Either[KafkaConsumerError, String] = {
    kafkaConsumer.flatMap(con => {
      def subscribe(): Unit = {
        con.unsubscribe()
        con.subscribe(List(topic).asJavaCollection)
      }

      kafkaTopic match {
        case Left(_) =>
          subscribe()
          topic.asRight
        case Right(existing) =>
          if (existing != topic) {
            subscribe()
            topic.asRight
          } else {
            existing.asRight
          }
      }
    })
  }

  private def newMessageBuffer(partitions: Seq[PartitionInfo]) : Map[Int, TopicPartitionBuffer[K, V]] = {
    partitions.map(p => (p.partition(), TopicPartitionBuffer[K, V](bufferSize))).toMap
  }

  private def moveOffset(offsetPosition: OffsetPosition, topic: String, partition: Option[Int] = None): Either[KafkaConsumerError, Unit] = {
    kafkaConsumer.map(con => {
      partition.fold(con.assignment().asScala.map(_.partition()).toSeq)(Seq(_)).map(p => {
        OffsetPosition(offsetPosition, topic, p)(con)
          .leftMap(offsetErr => KafkaConsumerError(offsetErr.reason))
      })
    })
  }

  private def pollMessages: UpdatedOffsets = Task {
    kafkaConsumer.map(con => {
      con.poll((5 seconds).toMillis).iterator().asScala.toSeq.groupBy(_.partition()).flatMap({
        case (partition, consumerRecords) =>
          messageBuffers.get(partition).map(buffer => {
            val crMap = consumerRecords.toOffsetMap
            buffer.add(crMap)
            (partition, crMap.keySet.max)
          })
      })
    })
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
  import ConsumerActor._

  var consumers: Map[String, ExistingConsumer] = Map()

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: Throwable => Stop
  }

  override def receive: Receive = {
    case CreateConsumer(conf) =>
      val consumer = getConsumer(conf.consumerId, conf.consumerType)
      consumer.consumerActor ! CreateConsumer(conf)
      sender ! consumer
  }

  private def getConsumer(consumerId: String, consumerType: ConsumerType): ExistingConsumer = {
    def name = s"${if (consumerType == StringConsumer) "string" else "avro"}-$consumerId"
    context.child(name).fold({
      consumerType match {
        case StringConsumer => ExistingConsumer(context.actorOf(Props[StringConsumerActor], name))
        case AvroConsumer => ExistingConsumer(context.actorOf(Props[AvroConsumerActor], name))
      }
    })(ExistingConsumer)
  }
}

object ConsumerSupervisorActor {

  case class ExistingConsumer(consumerActor: ActorRef)

}
