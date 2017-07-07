package models

import javax.annotation.concurrent.NotThreadSafe

import scala.collection.JavaConverters._

/**
  * A buffer that overwrites values in a sorted and contiguous nature.
  * When tuples are added, each element is added in ascending key-order with a preference to do so
  * in contiguous subgroups. The buffer retains a fixed upper-bounded size by evicting
  * already stored tuples, including those just inserted. When iterating through a contiguous group of
  * inserting tuples (which can include a single tuple), it will choose an stored tuple to evict in
  * the following order:
  *  {{{
  *    1. the tuple with the key closest to the right of the current contiguous subgroup, if it exists
  *    2. else, the tuple at the head of the buffer
  *  }}}
  *
  * @tparam K Key type
  * @tparam V Value type
  */
@NotThreadSafe
trait OverwritingBuffer[K, V] {
  import OverwritingBuffer._

  val maxSize: Int
  require(maxSize > 0, "maxSize of buffer must be greater than zero")

  private val buffer = new java.util.TreeMap[K, V]()

  def add(k: K, v: V)(implicit numeric: Numeric[K]): Unit = add(Map((k, v)))

  def add(records: Map[K, V])(implicit numeric: Numeric[K]): Unit = {

    groupContiguously(records.keys.toSeq).foreach(contiguousKeys => {

      lazy val (firstKey, lastKey) = contiguousKeys match {
        case Nil => (None, None)
        case head :: Nil => (Some(head), Some(head))
        case head :: tail => (Some(head), Some(tail.reverse.head))
      }

      def nextRemovableKey: K = {
        lazy val rightKey = lastKey.flatMap(k => Option(buffer.higherKey(k)))
        rightKey.fold[K](buffer.firstKey())(k => k)
      }

      contiguousKeys.foreach(key => {
        buffer.put(key, records(key))
        if (buffer.size() > maxSize) buffer.remove(nextRemovableKey)
      })
    })
  }

  def take(start: K, length: Int): Seq[V] = {
    val taken = buffer.tailMap(start).asScala.take(length)
    taken.keys.foreach(buffer.remove)
    taken.values.toSeq
  }

  def size: Int = buffer.size()
  def orderedValues: Seq[V] = buffer.values().asScala.toSeq
  def orderedKeys: Seq[K] = buffer.keySet().asScala.toSeq
}

object OverwritingBuffer {
  def groupContiguously[K](e: Seq[K])(implicit numeric: Numeric[K]): Seq[Seq[K]] = {
    import numeric._

    def appendOrStartNewGroup(group: Seq[K], k: K): Either[Seq[K], Seq[K]] = {
      group match {
        case Nil => Left(Seq(k))
        case head :: Nil => if (plus(head, one) == k) Left(Seq(head, k)) else Right(Seq(k))
        case g => if (plus(g.tail.reverse.head, one) == k) Left(g ++ Seq(k)) else Right(Seq(k))
      }
    }

    e.sorted.foldLeft(Seq[Seq[K]]()) {
      case (groups, key) => groups match {
        case Nil => Seq(Seq(key))
        case head :: Nil => appendOrStartNewGroup(head, key) match {
          case Left(first) => Seq(first)
          case Right(newGroup) => Seq(head, newGroup)
        }
        case g => appendOrStartNewGroup(g.tail.reverse.head, key) match {
          case Left(last) => g.dropRight(1) ++ Seq(last)
          case Right(newGroup) => g ++ Seq(newGroup)
        }
      }
    }
  }
}

object Buffers {

  import org.apache.kafka.clients.consumer.ConsumerRecord
  case class TopicPartitionBuffer[K, V](maxSize: Int) extends OverwritingBuffer[Long, ConsumerRecord[K, V]]

}