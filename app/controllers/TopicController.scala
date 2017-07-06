package controllers

import java.util.UUID
import javax.inject.{Inject, Named}

import actors.consumer.ConsumerActor.{ConsumerConfig, CreateConsumer}
import actors.consumer.{AtOffset, FromBeginning, FromEnd, OffsetPosition}
import akka.actor.ActorRef
import play.api.mvc.{InjectedController, Request}

trait TopicController extends InjectedController {

  val consumerRouter: ActorRef
  val consumerProps: java.util.Properties

  def renderTopic(topicName: String) = Action { request : Request[_] =>
    val offset = request.target.queryMap.get("offset").fold[OffsetPosition](FromEnd)(seq => seq.headOption.fold[OffsetPosition](FromEnd)(offsetPosition(_)))
    val consumerId = request.session.get("consumer_id").fold(UUID.randomUUID().toString)(s => s)

    consumerRouter ! CreateConsumer(ConsumerConfig(consumerId, consumerProps))


    Ok
  }

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

case class StringTopicController @Inject()(@Named("StringConsumerRouter") consumerRouter: ActorRef, consumerProps: java.util.Properties) extends TopicController
