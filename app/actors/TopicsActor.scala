package actors

import java.time.LocalDateTime
import javax.inject.Inject

import actors.TopicsActor._
import actors.consumer.genConsumerProps
import akka.actor.Actor
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.{ KafkaConsumer, ConsumerConfig => KafkaConsumerConfig }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.{ StringDeserializer, StringSerializer }

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object TopicsActor {

  val SyncDuration = 1 minute

  case object GetTopics
  case object SyncTopics
  case class CurrentTopics(topics: List[TopicInfo])

  case class TopicInfo(name: String, partitions: Int)

}

class TopicsActor @Inject() (baseConsumerProps: java.util.Properties) extends Actor {

  implicit val ec: ExecutionContext = context.dispatcher

  private val consumerProps = genConsumerProps(baseConsumerProps, Map(KafkaConsumerConfig.CLIENT_ID_CONFIG -> "offtopic-TopicsActor"))
  private var topics: List[TopicInfo] = List()

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(1 seconds)(self ! SyncTopics)
  }

  override def receive: Receive = {
    case GetTopics => sender ! CurrentTopics(List(topics:_*))
    case SyncTopics =>
      topics = syncTopics()
      context.system.scheduler.scheduleOnce(SyncDuration)(self ! SyncTopics)
  }

  private def syncTopics(): List[TopicInfo] = {
    val consumer = new KafkaConsumer[String, String](consumerProps, new StringDeserializer, new StringDeserializer)
    val topics = consumer.listTopics().asScala.map({
      case (topic, partitions) => TopicInfo(topic, partitions.asScala.map(_.partition()).max)
    }).toList.sortBy(_.name)
    consumer.close()
    topics
  }
}

// TODO remove this actor
class DummyDataActor extends Actor {

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  implicit val ec: ExecutionContext = context.dispatcher

  val randomTopics: Seq[String] = (0 to 25).map(i => s"Topic-number-${i.toString}")

  override def receive: Receive = {
    case _ => Unit
  }

  override def preStart(): Unit = {
    val zkClient = ZkUtils.createZkClient("server:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    try {
      randomTopics.foreach(topic => {
        AdminUtils.createTopic(zkUtils, topic, 1, 1)
      })
    } catch {
      case _: TopicExistsException => // skip topic creation
    }

    context.system.scheduler.schedule(5 second, 5 seconds, () => {
      try {
        val config: java.util.Map[String, AnyRef] = Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "server:9092").asInstanceOf[Map[String, AnyRef]].asJava
        val producer = new KafkaProducer[String, String](config, new StringSerializer(), new StringSerializer())
        randomTopics.foreach(topic => {
          producer.send(new ProducerRecord(topic, 0, "1", s"blah @ ${LocalDateTime.now()}"))
        })
        producer.flush()
        producer.close()
      } catch {
        case e: Throwable => println(e)
      }
    })
  }
}
