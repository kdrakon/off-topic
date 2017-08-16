package controllers

import javax.inject.{ Inject, Named }

import actors.TopicsActor.GetTopics
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import models.ApiMessages
import models.ApiMessages.CurrentTopics
import models.Session._
import play.api.libs.json.Json
import play.api.mvc.{ Action, AnyContent, InjectedController }

import scala.concurrent.ExecutionContext

class IndexController @Inject() (@Named("TopicsActor") topicsActor: ActorRef, assets: AssetsFinder)(implicit akkaTimeout: Timeout) extends InjectedController {

  implicit lazy val ec: ExecutionContext = defaultExecutionContext

  def index: Action[AnyContent] = Action { request =>
    Ok(views.html.index(assets)).withSession(request.session.getOffTopicSession.getOrElse(NewSession).toPlaySession)
  }

  def topics: Action[AnyContent] = Action.async { _ =>
    (topicsActor ? GetTopics).map {
      case currentTopics: CurrentTopics => Ok(Json.toJson(currentTopics)(ApiMessages.jsonformats.CurrentTopicsWrites))
      case _ => ServiceUnavailable
    }
  }

  def commit(topic: String, partition: Int, offset: Long): Action[AnyContent] = Action { request =>
    request.session.getOffTopicSession match {
      case None => BadRequest
      case Some(session) =>
        val updated = if (session.topic == topic) {
          session.copy(offsets = session.offsets.updated(partition, offset))
        } else {
          session.copy(topic = topic, offsets = Map())
        }
        Ok.withSession(updated.toPlaySession)
    }
  }
}
