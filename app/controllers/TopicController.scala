package controllers

import javax.inject.{Inject, Named}

import actors.consumer.ConsumerActor.StringConsumer
import actors.consumer.ConsumerSupervisorActor.ExistingConsumer
import actors.consumer.{AtOffset, FromBeginning, FromEnd, OffsetPosition}
import akka.actor.ActorRef
import akka.pattern._
import play.api.mvc.{Action, AnyContent, InjectedController, Request}
import org.apache.kafka.clients.consumer.{ConsumerConfig => KafkaConsumerConfig}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait TopicController extends InjectedController {
  import TopicController._
  import actors.consumer.ConsumerActor._

  implicit val timeout = akka.util.Timeout(30 seconds)
  implicit val executionContext: ExecutionContext

  val consumerType: ConsumerType
  val consumerSupervisor: ActorRef
  val baseConsumerProps: java.util.Properties

  def renderTopic(topicName: String): Action[AnyContent] = Action.async { request : Request[AnyContent] =>
    val offset = FromEnd // request.target.queryMap.get("offset").fold[OffsetPosition](FromEnd)(seq => seq.headOption.fold[OffsetPosition](FromEnd)(offsetPosition(_)))
    val consumerId = request.session.get("consumer_id").fold(genConsumerId)(s => s)

    val consumerProps = genConsumerProps(baseConsumerProps, Map(KafkaConsumerConfig.CLIENT_ID_CONFIG -> consumerId))
    val consumerConfig = ConsumerConfig(consumerId, consumerProps, consumerType)

    (consumerSupervisor ? CreateConsumer(consumerConfig)).map({
      case ExistingConsumer(consumerActor) =>
//        consumerActor ! StartConsumer(topicName, offset)
    }).map(_ => Ok.withSession("consumer_id" -> consumerId)).recover({
      case _: Throwable => InternalServerError
    })
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

case class StringTopicController @Inject()(@Named("ConsumerSupervisorActor") consumerSupervisor: ActorRef, baseConsumerProps: java.util.Properties, executionContext: ExecutionContext) extends TopicController {
  override val consumerType = StringConsumer
}
