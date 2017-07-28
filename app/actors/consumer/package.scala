package actors

import java.util.UUID

import scala.collection.JavaConverters._

package object consumer {

  def genConsumerId: String = UUID.randomUUID().toString

  def genConsumerProps(baseProps: java.util.Properties, overrides: Map[String, String]): java.util.Properties = {
    val props = new java.util.Properties()
    baseProps.stringPropertyNames().asScala.foreach(p => props.put(p, baseProps.get(p)))
    overrides.foreach(p => props.put(p._1, p._2))
    props
  }

}
