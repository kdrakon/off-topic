import javax.inject.{Named, Singleton}

import actors.consumer.ConsumerSupervisorActor
import akka.actor.{ActorRef, ActorSystem, Props}
import com.google.inject.{AbstractModule, Provides}
import com.typesafe.config.Config

import scala.collection.JavaConverters._

class Module extends AbstractModule {

  override def configure(): Unit = {}

  @Provides
  @Named("OffTopicConfig")
  def offTopicConfig(appConfig: Config): Config = appConfig.getConfig("offtopic")

  @Provides
  def defaultConsumerProps(@Named("OffTopicConfig") offTopicConfig: Config): java.util.Properties = {
    val defaultProps = offTopicConfig.getObject("consumer.defaultProps").unwrapped()
    val props = new java.util.Properties()
    defaultProps.asScala.foreach(kv => props.put(kv._1, kv._2.toString))
    props
  }

  @Provides
  @Singleton
  @Named("ConsumerSupervisorActor")
  def consumerSupervisorActor(actorSystem: ActorSystem, @Named("OffTopicConfig") offTopicConfig: Config): ActorRef = {
    actorSystem.actorOf(Props[ConsumerSupervisorActor])
  }
}
