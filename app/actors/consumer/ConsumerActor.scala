package actors.consumer

import java.util.{Properties, UUID}

import actors.consumer.ConsumerActor._
import actors.consumer.ConsumerSupervisorActor.ExistingConsumer
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy}
import cats.implicits._
import components.KafkaConsumerStream
import components.KafkaConsumerStream._
import monix.eval.{MVar, Task}
import monix.execution.Scheduler
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.reactivestreams.{Subscription, Subscriber => ReactiveSubscriber}

import scala.collection.JavaConverters._

object ConsumerActor {

  sealed trait ConsumerType
  case object StringConsumer extends ConsumerType
  case object AvroConsumer extends ConsumerType

  case class ConsumerConfig(consumerId: String, props: Properties, consumerType: ConsumerType)
  case class CreateConsumer(conf: ConsumerConfig)
  case object ShutdownConsumer
  case class StartConsumer(topic: String, offsetStart: OffsetPosition)
  case class MoveOffset(topic: String, partition: Int, offsetPosition: OffsetPosition)

  case object GetMessages
  case class MessagesPayload[K,V](payload: ConsumerRecords[K, V])
  case object EndOfMessageBuffer

  def genConsumerId: String = UUID.randomUUID().toString

  def genConsumerProps(baseProps: java.util.Properties, overrides: Map[String, String]): java.util.Properties = {
    val props = new java.util.Properties()
    baseProps.stringPropertyNames().asScala.foreach(p => props.put(p, baseProps.get(p)))
    overrides.foreach(p => props.put(p._1, p._2))
    props
  }
}

trait ConsumerActor[K, V] extends Actor {
  import ConsumerActor._

  implicit private val scheduler = Scheduler(context.dispatcher)
  private var kafkaConsumerStream: ConfiguredKafkaConsumerStream[K, V] = KafkaConsumerError("Stream not yet created").asLeft
  private var kafkaTopic: ConfiguredKafkaTopic = KafkaConsumerError("Topic not yet set").asLeft
  implicit private val messageSubscriber: ReactiveSubscriber[PolledConsumerRecords[K, V]] = new ReactiveSubscriber[PolledConsumerRecords[K, V]] {
    override def onError(t: Throwable): Unit = ???
    override def onComplete(): Unit = self ! ShutdownConsumer
    override def onNext(t: PolledConsumerRecords[K, V]): Unit = t.map(cr => MessagesPayload(cr)).foreach(mp => outboundMessages.put(mp).runAsync)
    override def onSubscribe(s: Subscription): Unit = messageSubscription = s.asRight
  }
  private var messageSubscription: Either[KafkaConsumerError, Subscription] = KafkaConsumerError("No subscription").asLeft
  private val outboundMessages: MVar[MessagesPayload[K, V]] = MVar.empty

  override def receive: Receive = {

    case CreateConsumer(conf) =>
      kafkaConsumerStream = kafkaConsumerStream.fold(_ => KafkaConsumerStream.create(createConsumer(conf)), stream => stream.asRight)

    case StartConsumer(topic, offsetPosition) =>
      val source = sender
      kafkaConsumerStream.map(stream => {
        val task = for {
          _1 <- stream.setTopic(kafkaTopic, topic) // TODO set kafkaTopic
          _2 <- stream.moveOffset(offsetPosition, topic)
          _3 <- Task(stream.start())
        } yield {
          _3
        }
        task.doOnFinish({
          case None => Task(self.tell(GetMessages, source))
          case Some(t) => ???
        }).runAsync
      })

    case ShutdownConsumer =>
      kafkaConsumerStream.map(_.shutdown())
      self ! PoisonPill

    case m: MoveOffset =>
      kafkaConsumerStream.map(_.moveOffset(m.offsetPosition, m.topic, Some(m.partition)).runAsync)

    case GetMessages =>
      val recipient = sender
      messageSubscription.map(_.request(1L))
      outboundMessages.take.map(m => recipient ! m).runAsync
//      messageBuffers.get(partition).foreach(buffer => {
//        sender ! MessagesPayload(buffer.take(offset, maxMessages))
//      })

//    case PollMessages if polling && messageBuffers.totalBufferSize < maxMessageBuffersSize =>
//      pollMessages.map(_.map(offsetMap ++= _))
//        .doOnFinish(_ => Task(context.system.scheduler.scheduleOnce(pollInterval, self, PollMessages))).runAsync
  }

  def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[K, V]

//  private def shutdownConsumer(kafkaConsumer: ConfiguredKafkaConsumer): ConfiguredKafkaConsumer = {
//    kafkaConsumer.foreach(con => {
//      con.unsubscribe()
//      con.close()
//      polling = false
//    })
//    KafkaConsumerError("Consumer was shutdown and no longer usable").asLeft
//  }

//  private def startConsumer(start: StartConsumer): Either[KafkaConsumerError, Unit] = {
//    kafkaConsumer.flatMap(con => {
//      kafkaTopic = subscribeToTopic(start.topic)
//      messageBuffers = newMessageBuffer(con.partitionsFor(start.topic).asScala)
//      offsetMap = Map()
//      polling = true
//      (self ! PollMessages).asRight
//    }).flatMap(_ => moveOffset(start.offsetStart, start.topic))
//  }

//  private def subscribeToTopic(topic: String): Either[KafkaConsumerError, String] = {
//    kafkaConsumer.flatMap(con => {
//      def subscribe(): Unit = {
//        con.unsubscribe()
//        con.subscribe(List(topic).asJavaCollection)
//      }
//
//      kafkaTopic match {
//        case Left(_) =>
//          subscribe()
//          topic.asRight
//        case Right(existing) =>
//          if (existing != topic) {
//            subscribe()
//            topic.asRight
//          } else {
//            existing.asRight
//          }
//      }
//    })
//  }

//  private def newMessageBuffer(partitions: Seq[PartitionInfo]) : Map[Int, TopicPartitionBuffer[K, V]] = {
//    partitions.map(p => (p.partition(), TopicPartitionBuffer[K, V](bufferSize))).toMap
//  }

//  private def moveOffset(offsetPosition: OffsetPosition, topic: String, partition: Option[Int] = None): Either[KafkaConsumerError, Unit] = {
//    kafkaConsumer.map(con => {
//      partition.fold(con.assignment().asScala.map(_.partition()).toSeq)(Seq(_)).map(p => {
//        OffsetPosition(offsetPosition, topic, p)(con)
//          .leftMap(offsetErr => KafkaConsumerError(offsetErr.reason))
//      })
//    })
//  }

//  private def pollMessages: UpdatedOffsets = Task {
//    kafkaConsumer.map(con => {
//      con.poll((5 seconds).toMillis).iterator().asScala.toSeq.groupBy(_.partition()).flatMap({
//        case (partition, consumerRecords) =>
//          messageBuffers.get(partition).map(buffer => {
//            val crMap = consumerRecords.toOffsetMap
//            buffer.add(crMap)
//            (partition, crMap.keySet.max)
//          })
//      })
//    })
//  }
}

class StringConsumerActor extends ConsumerActor[String, String] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[String, String] = {
    try {
      val consumer = new KafkaConsumer[String, String](conf.props, new StringDeserializer(), new StringDeserializer())
      consumer.asRight
    } catch {
      case t: Throwable => KafkaConsumerError("Failed to create consumer", Some(t)).asLeft
    }
  }
}

class AvroConsumerActor extends ConsumerActor[Any, GenericRecord] {
  override def createConsumer(conf: ConsumerConfig): ConfiguredKafkaConsumer[Any, GenericRecord] = {
    ???
  }
}

class ConsumerSupervisorActor extends Actor {
  import ConsumerActor._

  var consumers: Map[String, ExistingConsumer] = Map()

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: Throwable => Stop
  }

  override def receive: Receive = {
    case CreateConsumer(conf) =>
      val consumer = getConsumer(conf.consumerId, conf.consumerType)
      consumer.consumerActor ! CreateConsumer(conf)
      sender ! consumer
  }

  private def getConsumer(consumerId: String, consumerType: ConsumerType): ExistingConsumer = {
    def name = s"${if (consumerType == StringConsumer) "string" else "avro"}-$consumerId"
    context.child(name).fold({
      consumerType match {
        case StringConsumer => ExistingConsumer(context.actorOf(Props[StringConsumerActor], name))
        case AvroConsumer => ExistingConsumer(context.actorOf(Props[AvroConsumerActor], name))
      }
    })(ExistingConsumer)
  }
}

object ConsumerSupervisorActor {

  case class ExistingConsumer(consumerActor: ActorRef)

}
