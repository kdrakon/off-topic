package models

import play.api.libs.json.{ Json, Writes }

object ApiMessages {

  case class CurrentTopics(topics: List[TopicInfo])
  case class TopicInfo(name: String, partitions: Int)

  object jsonformats {

    implicit val TopicInfoWrites: Writes[TopicInfo] = Json.writes[TopicInfo]
    implicit val CurrentTopicsWrites: Writes[CurrentTopics] = Json.writes[CurrentTopics]

  }


}
