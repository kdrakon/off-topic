package actors.consumer

import akka.actor.{ Actor, ActorRef, PoisonPill }
import cats.implicits._
import components.KafkaConsumerStream._
import components.{ AtOffset, FromBeginning, KafkaConsumerStream }
import models.ConsumerMessages._
import monix.execution.Scheduler
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

object ConsumerActor {

  sealed trait ConsumerType
  case object StringConsumer extends ConsumerType
  case object AvroConsumer extends ConsumerType

}

trait ConsumerActor[K, V] extends Actor {

  protected val outboundSocketActor: ActorRef
  protected val consumerConfig: ConsumerConfig

  implicit private val scheduler = Scheduler(context.dispatcher)
  private var kafkaConsumerStream: ConfiguredKafkaConsumerStream[K, V] = KafkaConsumerError("Stream not yet created").asLeft

  def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[K, V]

  override def preStart(): Unit = {
    self ! CreateConsumer(consumerConfig)
  }

  override def postStop(): Unit = {
    kafkaConsumerStream.map(_.stop().runAsync) // in the event that the client abruptly disconnected
  }

  override def receive: Receive = {

    case CreateConsumer(conf) =>
      kafkaConsumerStream = kafkaConsumerStream.fold(_ => KafkaConsumerStream.create(createConsumer(conf).map(_.withSubscribedTopic(conf.topic))), stream => stream.asRight)
      kafkaConsumerStream.foreach(_ => {
        if (conf.offsets.map.nonEmpty) {
          conf.offsets.map.map(o => MoveOffset(APartition(o._1), AtOffset(o._2))).foreach(m => self ! m)
        } else {
          self ! MoveOffset(AllPartitions, FromBeginning)
        }
      })

    case m: MoveOffset =>
      kafkaConsumerStream.map(stream => {
        stream.moveOffset(m.offsetPosition, consumerConfig.topic, m.partition)
          .map(e => e.leftMap(err => outboundSocketActor ! err))
          .runAsync
      })

    case PollConsumer =>
      kafkaConsumerStream.foreach(stream => {
        stream.poll.map({
          case Right(m) if !m.payload.isEmpty => outboundSocketActor ! m
          case Left(e) => outboundSocketActor ! e
        }).runAsync
      })

    case ShutdownConsumer =>
      kafkaConsumerStream.map(_.stop().runAsync)
      self ! PoisonPill
  }
}

class StringConsumerActor(val outboundSocketActor: ActorRef, val consumerConfig: ConsumerConfig) extends ConsumerActor[String, String] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[String, String] = {
    try {
      val consumer = new KafkaConsumer[String, String](conf.props, new StringDeserializer(), new StringDeserializer())
      consumer.asRight
    } catch {
      case t: Throwable => KafkaConsumerError("Failed to create consumer", Some(t)).asLeft
    }
  }
}

class AvroConsumerActor(val outboundSocketActor: ActorRef, val consumerConfig: ConsumerConfig) extends ConsumerActor[Any, GenericRecord] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[Any, GenericRecord] = {
    ???
  }
}

