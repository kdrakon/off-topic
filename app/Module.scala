import javax.inject.{ Named, Singleton }

import actors.{ DummyDataActor, TopicsActor }
import com.google.inject.{ AbstractModule, Provides }
import com.typesafe.config.Config
import play.api.libs.concurrent.AkkaGuiceSupport

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class Module extends AbstractModule with AkkaGuiceSupport {

  override def configure(): Unit = {

    bindActor[DummyDataActor]("DummyActor") // TODO remove
    bindActor[TopicsActor]("TopicsActor")
  }

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
  def defaultAkkaTimeout = akka.util.Timeout(30 seconds)
}
