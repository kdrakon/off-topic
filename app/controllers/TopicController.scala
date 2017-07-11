package controllers

import javax.inject.{Inject, Named}

import actors.consumer.ConsumerActor.StringConsumer
import actors.consumer._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.Materializer
import cats.implicits._
import models.ConsumerMessages.ConsumerMessage
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.kafka.clients.consumer.{ConsumerConfig => KafkaConsumerConfig}
import play.api.libs.streams.ActorFlow
import play.api.mvc.WebSocket.MessageFlowTransformer
import play.api.mvc._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait TopicController extends InjectedController {
  import actors.consumer.ConsumerActor._

  implicit val timeout = akka.util.Timeout(30 seconds)
  implicit val actorSystem: ActorSystem
  implicit val mat: Materializer
  implicit val executionContext: ExecutionContext
  implicit val scheduler: Scheduler = Scheduler(executionContext)

  val consumerType: ConsumerType
  val baseConsumerProps: java.util.Properties

//  def renderTopic(topicName: String): Action[AnyContent] = Action.async { request : Request[AnyContent] =>
//    val offset = FromEnd // request.target.queryMap.get("offset").fold[OffsetPosition](FromEnd)(seq => seq.headOption.fold[OffsetPosition](FromEnd)(offsetPosition(_)))
//    val consumerId = request.session.get("consumer_id").fold(genConsumerId)(s => s)
//
//    val consumerProps = genConsumerProps(baseConsumerProps, Map(KafkaConsumerConfig.CLIENT_ID_CONFIG -> consumerId))
//    val consumerConfig = ConsumerConfig(consumerId, consumerProps, consumerType)
//
//    (consumerSupervisor ? CreateConsumer(consumerConfig)).map({
//      case ExistingConsumer(consumerActor) =>
//        consumerActor ! StartConsumer(topicName, offset)
//    }).map(_ => Ok.withSession("consumer_id" -> consumerId)).recover({
//      case _: Throwable => InternalServerError
//    })
//  }

  import models.ConsumerMessages.jsonformats._
  implicit val messageFlowTransformer = MessageFlowTransformer.jsonMessageFlowTransformer[ConsumerMessage, ConsumerMessage]

  def topicSocket(topicName: String): WebSocket = WebSocket.acceptOrResult[ConsumerMessage, ConsumerMessage] { request =>
    Task {
      ActorFlow.actorRef(outboundSocketActor => {
        Props(classOf[StringConsumerActor], outboundSocketActor)
      }).asRight
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
}

case class StringTopicController @Inject()
  (
    baseConsumerProps: java.util.Properties,
    executionContext: ExecutionContext,
    _actorSystem: ActorSystem,
    _mat: Materializer
  ) extends TopicController {
  override val consumerType = StringConsumer
  override implicit val actorSystem = _actorSystem
  override implicit val mat = _mat
}
