package controllers

import javax.inject.{ Inject, Named }

import actors.TopicsActor.{ CurrentTopics, GetTopics }
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import play.api.mvc.{ Action, AnyContent, InjectedController }

import scala.concurrent.ExecutionContext

class IndexController @Inject() (@Named("TopicsActor") topicsActor: ActorRef, assets: AssetsFinder)(implicit akkaTimeout: Timeout) extends InjectedController {

  implicit lazy val ec: ExecutionContext = defaultExecutionContext

  def index: Action[AnyContent] = Action.async { _ =>
    (topicsActor ? GetTopics).map {
      case CurrentTopics(topics) => Ok(views.html.index(assets, topics))
      case _ => ServiceUnavailable
    }
  }

}
