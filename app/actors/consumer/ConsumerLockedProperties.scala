package actors.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig


object ConsumerLockedProperties {

  // These are properties for Consumers that can never/should never be overwritten
  private val properties =
    Map(
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

  lazy val LockedProperties: Properties = {
    val p = new java.util.Properties()
    properties.foreach(pair => p.setProperty(pair._1, pair._2))
    p
  }

  implicit class Implicits(props: Properties) {
    def withLockedOverrides: Properties = {
      val withOverriden = new java.util.Properties(props)
      properties.foreach(pair => withOverriden.replace(pair._1, pair._2))
      withOverriden
    }
  }
}
