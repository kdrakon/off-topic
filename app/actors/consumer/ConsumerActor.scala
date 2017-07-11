package actors.consumer

import java.util.UUID

import akka.actor.{Actor, ActorRef, PoisonPill}
import cats.implicits._
import components.KafkaConsumerStream
import components.KafkaConsumerStream._
import models.ConsumerMessages._
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object ConsumerActor {

  sealed trait ConsumerType
  case object StringConsumer extends ConsumerType
  case object AvroConsumer extends ConsumerType

  def genConsumerId: String = UUID.randomUUID().toString

  def genConsumerProps(baseProps: java.util.Properties, overrides: Map[String, String]): java.util.Properties = {
    val props = new java.util.Properties()
    baseProps.stringPropertyNames().asScala.foreach(p => props.put(p, baseProps.get(p)))
    overrides.foreach(p => props.put(p._1, p._2))
    props
  }
}

trait ConsumerActor[K, V] extends Actor {

  val outboundSocketActor: ActorRef

  implicit private val scheduler = Scheduler(context.dispatcher)
  private var kafkaConsumerStream: ConfiguredKafkaConsumerStream[K, V] = KafkaConsumerError("Stream not yet created").asLeft
  private var kafkaTopic: ConfiguredKafkaTopic = KafkaConsumerError("Topic not yet set").asLeft

  def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[K, V]

  override def receive: Receive = {

    case CreateConsumer(conf) =>
      kafkaConsumerStream = kafkaConsumerStream.fold(_ => KafkaConsumerStream.create(createConsumer(conf)), stream => stream.asRight)

    case StartConsumer(topic, offsetPosition) =>
      val source = sender
      kafkaConsumerStream.map(stream => {
        val task = for {
          _1 <- stream.setTopic(kafkaTopic, topic) // TODO set kafkaTopic
          _2 <- stream.moveOffset(offsetPosition, topic)
          _3 <- Task(stream.start(???))
        } yield {
          _3
        }
        task.doOnFinish({
          case None => ???
          case Some(t) => ???
        }).runAsync
      })

    case ShutdownConsumer =>
      kafkaConsumerStream.map(_.shutdown())
      self ! PoisonPill

    case m: MoveOffset =>
      kafkaConsumerStream.map(_.moveOffset(m.offsetPosition, m.topic, Some(m.partition)).runAsync)
  }
}

class StringConsumerActor(val outboundSocketActor: ActorRef) extends ConsumerActor[String, String] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[String, String] = {
    try {
      val consumer = new KafkaConsumer[String, String](conf.props, new StringDeserializer(), new StringDeserializer())
      consumer.asRight
    } catch {
      case t: Throwable => KafkaConsumerError("Failed to create consumer", Some(t)).asLeft
    }
  }
}

class AvroConsumerActor(val outboundSocketActor: ActorRef) extends ConsumerActor[Any, GenericRecord] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[Any, GenericRecord] = {
    ???
  }
}

