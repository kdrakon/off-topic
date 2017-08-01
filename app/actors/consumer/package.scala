package actors

import java.util.UUID

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

package object consumer {

  def genConsumerId: String = UUID.randomUUID().toString

  def genConsumerProps(baseProps: java.util.Properties, overrides: Map[String, String]): java.util.Properties = {
    val props = new java.util.Properties()
    baseProps.stringPropertyNames().asScala.foreach(p => props.put(p, baseProps.get(p)))
    overrides.foreach(p => props.put(p._1, p._2))
    props
  }

  implicit class KafkaConsumerImplicits[K, V](kafkaConsumer: KafkaConsumer[K, V]) {

    def withSubscribedTopic(topic: String): KafkaConsumer[K, V] = {
      kafkaConsumer.unsubscribe()
      val partitions = kafkaConsumer.partitionsFor(topic).asScala.map(p => new TopicPartition(p.topic(), p.partition())).asJavaCollection
      kafkaConsumer.assign(partitions)
      kafkaConsumer
    }
  }
}
