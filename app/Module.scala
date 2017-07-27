import javax.inject.Named

import actors.DummyDataActor
import com.google.inject.{ AbstractModule, Provides }
import com.typesafe.config.Config
import play.api.libs.concurrent.AkkaGuiceSupport

import scala.collection.JavaConverters._

class Module extends AbstractModule with AkkaGuiceSupport {

  override def configure(): Unit = {
    bindActor[DummyDataActor]("DummyActor")
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
}
