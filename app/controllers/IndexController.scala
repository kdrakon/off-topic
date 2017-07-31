package controllers

import javax.inject.{ Inject, Named }

import actors.TopicsActor.GetTopics
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import models.ApiMessages.CurrentTopics
import play.api.libs.json.Json
import play.api.mvc.{ Action, AnyContent, InjectedController }

import scala.concurrent.ExecutionContext

class IndexController @Inject() (@Named("TopicsActor") topicsActor: ActorRef, assets: AssetsFinder)(implicit akkaTimeout: Timeout) extends InjectedController {

  import models.ApiMessages.jsonformats._

  implicit lazy val ec: ExecutionContext = defaultExecutionContext

  def index: Action[AnyContent] = Action { _ => Ok(views.html.index(assets)) }

  def topics: Action[AnyContent] = Action.async { _ =>
    (topicsActor ? GetTopics).map {
      case currentTopics: CurrentTopics => Ok(Json.toJson(currentTopics))
      case _ => ServiceUnavailable
    }
  }

}
