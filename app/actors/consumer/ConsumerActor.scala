package actors.consumer

import java.util.Properties

import actors.consumer.ConsumerActor._
import actors.consumer.ConsumerLockedProperties._
import akka.actor.{Actor, ActorContext, PoisonPill, Props}
import akka.routing.ConsistentHashingPool
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import cats.implicits._
import models.OffsetPosition
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object ConsumerActor {

  case class ConsumerConfig(id: String, props: Properties)
  case class CreateConsumer(conf: ConsumerConfig) extends ConsistentHashable {
    override def consistentHashKey = conf.id
  }
  case class CreateAvroConsumer(conf: ConsumerConfig) extends ConsistentHashable {
    override def consistentHashKey = conf.id
  }

  case class ShutdownConsumer(id: String) extends ConsistentHashable {
    override def consistentHashKey = id
  }
  case class StartConsumer(topic: String, partition: Int, offsetStart: OffsetPosition)
  case class MoveOffset(topic: String, partition: Int, offsetPosition: OffsetPosition)
  case object PollMessages
  case class GetMessages(maxMessages: Int)
  case class MessagesPayload[K, V](messages: Seq[ConsumerRecord[K, V]])
}

trait ConsumerActor[K, V] extends Actor {

  case class KafkaConsumerError(reason: String, error: Option[Throwable] = None)
  type ConfiguredKafkaConsumer = Either[KafkaConsumerError, KafkaConsumer[K, V]]
  type MessageBuffer = Seq[ConsumerRecord[K, V]]
  type PolledMessages = Task[Either[KafkaConsumerError, ConsumerRecords[K, V]]]

  implicit val scheduler = Scheduler(context.dispatcher)

  private var kafkaConsumer: ConfiguredKafkaConsumer = KafkaConsumerError("Consumer not yet created").asLeft
  private var polling: Boolean = false
  private val pollInterval = 500 millis
  private val minimumBufferSize = 1000
  private var messageBuffer: MessageBuffer = Seq()

  override def receive: Receive = {
    case CreateConsumer(conf) => kafkaConsumer = createConsumer(conf)
    case ShutdownConsumer(_) => shutdownConsumer()
    case _: StartConsumer => startConsumer(_)
    case _: MoveOffset => moveOffset(_)
    case GetMessages(maxMessages) =>
      val payload = MessagesPayload(messageBuffer.take(maxMessages))
      messageBuffer = messageBuffer.takeRight(messageBuffer.size - payload.messages.size)
      sender ! payload
    case PollMessages if polling && messageBuffer.size < minimumBufferSize =>
      pollMessages.map(_.map(records => {
        messageBuffer = messageBuffer ++ records.iterator().asScala.toSeq
      })).doOnFinish(_ => Task(context.system.scheduler.scheduleOnce(pollInterval, self, PollMessages))).runAsync
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
    kafkaConsumer.flatMap(con =>{
      con.unsubscribe()
      con.subscribe(List(start.topic).asJavaCollection)
      polling = true
      messageBuffer = Seq()
      self ! PollMessages
      OffsetPosition(start.offsetStart, start.topic, start.partition)(con)
        .leftMap(offsetErr => KafkaConsumerError(offsetErr.reason))
    })
  }

  private def moveOffset(moveOffset: MoveOffset): Either[KafkaConsumerError, Unit] = {
    kafkaConsumer.map(con => {
      messageBuffer = Seq()
      OffsetPosition(moveOffset.offsetPosition, moveOffset.topic, moveOffset.partition)(con)
        .leftMap(offsetErr => KafkaConsumerError(offsetErr.reason))
    })
  }

  private def pollMessages: PolledMessages = Task {
    kafkaConsumer.map(con => {
      con.poll((5 seconds).toMillis)
    })
  }
}

class StringConsumerActor extends ConsumerActor[String, String] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer = {
    try {
      val consumer = new KafkaConsumer[String, String](conf.props.withLockedOverrides, new StringDeserializer(), new StringDeserializer())
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

sealed abstract class ConsumerRouter(context: ActorContext, routerPoolSize: Int) {
  def props: Props
  val router = context.actorOf(ConsistentHashingPool(routerPoolSize).props(props))
}
final class StringConsumerRouter(context: ActorContext, routerPoolSize: Int) extends ConsumerRouter(context, routerPoolSize) {
  override def props = Props[StringConsumerActor]
}
final class AvroConsumerRouter(context: ActorContext, routerPoolSize: Int) extends ConsumerRouter(context, routerPoolSize) {
  override def props = Props[AvroConsumerActor]
}
