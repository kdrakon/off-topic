package controllers

import javax.inject.Inject

import actors.consumer.ConsumerActor.{ AvroConsumer, ConsumerType, StringConsumer }
import actors.consumer._
import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.stream.Materializer
import cats.implicits._
import components.{ AtOffset, FromBeginning, FromEnd, OffsetPosition }
import models.ConsumerMessages.{ ConsumerConfig, ConsumerMessage }
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.kafka.clients.consumer.{ ConsumerConfig => KafkaConsumerConfig }
import play.api.libs.streams.ActorFlow
import play.api.mvc.WebSocket.MessageFlowTransformer
import play.api.mvc._

import scala.concurrent.ExecutionContext

trait TopicController extends BaseController {
  import TopicController._
  import actors.consumer.ConsumerActor._

  implicit val actorSystem: ActorSystem
  implicit val mat: Materializer
  implicit lazy val executionContext: ExecutionContext = defaultExecutionContext
  implicit val scheduler: Scheduler = Scheduler(executionContext)

  val consumerType: ConsumerType
  val baseConsumerProps: java.util.Properties

  import models.ConsumerMessages.jsonformats._
  implicit val messageFlowTransformer = MessageFlowTransformer.jsonMessageFlowTransformer[ConsumerMessage, ConsumerMessage]

  def topicSocket(topicName: String): WebSocket = WebSocket.acceptOrResult[ConsumerMessage, ConsumerMessage] { request =>

    val consumerId = s"Off-Topic-Consumer-${request.session.get("consumer_id").fold(genConsumerId)(s => s)}"
    val consumerProps = genConsumerProps(baseConsumerProps, Map(KafkaConsumerConfig.CLIENT_ID_CONFIG -> consumerId))
    val consumerConfig = ConsumerConfig(consumerId, topicName, consumerProps, consumerType)

    Task {
      ActorFlow.actorRef(outboundSocketActor => props(consumerType, outboundSocketActor, consumerConfig)).asRight
    }.runAsync
  }
}

object TopicController{

  val offsetPosition: String => OffsetPosition = {
    case "beginning" => FromBeginning
    case "end" => FromEnd
    case offset =>
      try {
        AtOffset(offset.toLong)
      } catch {
        case _: java.lang.NumberFormatException => FromEnd
      }
  }

  def props(consumerType: ConsumerType, outboundSocketActor: ActorRef, consumerConfig: ConsumerConfig): Props = {
    val actorClass = consumerType match {
      case StringConsumer => classOf[StringConsumerActor]
      case AvroConsumer => classOf[AvroConsumerActor]
    }
    Props(actorClass, outboundSocketActor, consumerConfig)
  }
}

case class StringTopicController @Inject()
  (
    baseConsumerProps: java.util.Properties,
    _actorSystem: ActorSystem,
    _mat: Materializer,
    controllerComponents: ControllerComponents
  ) extends TopicController {
  override val consumerType = StringConsumer
  override implicit val actorSystem = _actorSystem
  override implicit val mat = _mat
}

case class AvroTopicController @Inject()
(
  baseConsumerProps: java.util.Properties,
  _actorSystem: ActorSystem,
  _mat: Materializer,
  controllerComponents: ControllerComponents
  ) extends TopicController {
  override val consumerType = AvroConsumer
  override implicit val actorSystem = _actorSystem
  override implicit val mat = _mat
}
