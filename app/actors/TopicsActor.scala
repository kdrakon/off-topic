package actors

import java.time.LocalDateTime

import actors.TopicsActor.GetAllTopics
import akka.actor.Actor
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.ExecutionContext

object TopicsActor {

  case object GetAllTopics

}

class TopicsActor extends Actor {

  override def receive: Receive = {
    case GetAllTopics => ???
  }
}

// TODO remove this actor
class DummyDataActor extends Actor {

  import scala.concurrent.duration._
  import scala.collection.JavaConverters._

  implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case _ => Unit
  }

  override def preStart(): Unit = {
    val zkClient = ZkUtils.createZkClient("server:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    try {
      AdminUtils.createTopic(zkUtils, "test", 1, 1)
    } catch {
      case _: TopicExistsException => // skip topic creation
    }

    context.system.scheduler.schedule(5 second, 5 seconds, () => {
      try {
        val config: java.util.Map[String, AnyRef] = Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "server:9092").asInstanceOf[Map[String, AnyRef]].asJava
        val producer = new KafkaProducer[String, String](config, new StringSerializer(), new StringSerializer())
        producer.send(new ProducerRecord("test", 0, "1", s"blah @ ${LocalDateTime.now()}"))
        producer.flush()
        producer.close()
      } catch {
        case e: Throwable => println(e)
      }
    })
  }
}
