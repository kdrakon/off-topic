package actors.consumer

import akka.actor.{ Actor, ActorRef, PoisonPill }
import cats.implicits._
import components.KafkaConsumerStream
import components.KafkaConsumerStream._
import models.ConsumerMessages._
import monix.eval.Task
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
    kafkaConsumerStream.map(_.stop()) // in the event that the client abruptly disconnected
  }

  override def receive: Receive = {

    case CreateConsumer(conf) =>
      kafkaConsumerStream = kafkaConsumerStream.fold(_ => KafkaConsumerStream.create(createConsumer(conf)), stream => stream.asRight)

    case StartConsumer(offsetPosition) =>
      kafkaConsumerStream.map(stream => {

        def rightToUnit[T](e: Either[KafkaConsumerError, T]) = e.map(_ => ())

        val setup = Task.sequence(List(stream.setTopic(consumerConfig.topic).map(rightToUnit), stream.moveOffset(offsetPosition, consumerConfig.topic).map(rightToUnit)))
        val start = setup.map(_.sequenceU).map({
          case Left(error) => outboundSocketActor ! error
          case Right(_) => stream.start(outboundSocketActor)
        })

        start.doOnFinish({
          case None => Task.unit // do nothing
          case Some(t) => Task(outboundSocketActor ! KafkaConsumerError("Encountered error starting Kafka Consumer stream", Some(t)))
        }).runAsync
      })

    case ShutdownConsumer =>
      kafkaConsumerStream.map(_.stop())
      self ! PoisonPill

    case m: MoveOffset =>
      kafkaConsumerStream.map(stream => {
        stream.moveOffset(m.offsetPosition, consumerConfig.topic, m.partition)
          .map(e => e.leftMap(err => outboundSocketActor ! err))
          .runAsync
      })
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

