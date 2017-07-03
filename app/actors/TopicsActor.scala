package actors

import actors.TopicsActor.GetAllTopics
import akka.actor.Actor

object TopicsActor {

  case object GetAllTopics

}

class TopicsActor extends Actor {

  override def receive: Receive = {
    case GetAllTopics => ???
  }
}
