package models

import java.util.UUID

import play.api.libs.json._
import play.api.mvc.{ Session => PlaySession }


case class Session
(
  consumerId: String,
  topic: String = "",
  offsets: Map[Int, Long] = Map()
)

object Session {

  val PlaySessionKey = "OffTopicSession"

  val SessionFormat : Format[Session] = Format(

    Reads[Session](json => {
      val read =
        for {
          consumerId <- json.as[JsObject].value.get("consumerId").map(_.as[String])
          topic <- json.as[JsObject].value.get("topic").map(_.as[String])
          offsets <- json.as[JsObject].value.get("offsets")
        } yield {
          val of = offsets.as[JsObject].value.map({
            case (partition, offset) => partition.toInt -> offset.as[String].toLong
          }).toMap
          JsSuccess(Session(consumerId, topic, of))
        }
      read.getOrElse(JsError("Invalid OffTopicSession"))
    }),

    Writes[Session](session => {
      val jsonOffsets = session.offsets.map({
        case (partition, offset) => partition.toString -> JsString(offset.toString)
      })

      Json.obj(
        "consumerId" -> JsString(session.consumerId),
        "topic" -> JsString(session.topic),
        "offsets" -> JsObject(jsonOffsets)
      )
    })
  )

  def NewSession = Session(consumerId = s"Off-Topic-Consumer-${UUID.randomUUID().toString}")

  implicit class SessionImplicits(session: Session) {
    def toJson: JsValue = SessionFormat.writes(session)
    def toPlaySession: PlaySession = PlaySession(Map(PlaySessionKey -> session.toJson.toString))
  }

  implicit class PlaySessionImplicits(playSession: PlaySession) {
    def getOffTopicSession: Option[Session] = {
      playSession.get(PlaySessionKey).flatMap(s => SessionFormat.reads(Json.parse(s)).asOpt)
    }
  }
}