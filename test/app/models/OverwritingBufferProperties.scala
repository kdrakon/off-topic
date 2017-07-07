package app.models

import models.OverwritingBuffer
import org.scalacheck.Prop._
import org.scalacheck._
import org.scalatest.WordSpec

import scala.annotation.tailrec

case class LongStringBuffer(maxSize: Int) extends OverwritingBuffer[Long, String]

class OverwritingBufferProperties extends Properties(OverwritingBuffer.getClass.getCanonicalName) {
  import OverwritingBufferTest._

  val maxSize = Gen.Choose.chooseInt.choose(1, 50)
  def records(n: Int) = Gen.mapOfN(n, for {
    key <- Gen.Choose.chooseLong.choose(-1L, Long.MaxValue)
    value <- Gen.alphaNumStr
  } yield {
    (key, value)
  })

  val insertsRecords = forAll(maxSize) { size =>
    (size > 0) ==> {
      val buffer = LongStringBuffer(size)
      forAll(Gen.Choose.chooseInt.choose(size, size * 2)) { n =>
        (n > 0) ==> {
          val insert = records(n).sample.get
          buffer.add(insert)
          val isSizeRestricted = buffer.size <= size
          all(isSizeRestricted) :| "inserts records"
        }
      }
    }
  }

  val evictsRecords = forAll(maxSize) { size =>
    (size > 10) ==> {
      val originalKeys = Range(0, size, 2).map(_.toLong) // even numbered keys
      val buffer = LongStringBuffer(originalKeys.size)
      buffer.add(originalKeys.map((_, "")).toMap)

      buffer.add(0, "X")
      val noEviction = buffer.orderedKeys == originalKeys

      val lastKey = buffer.orderedKeys.tail.reverse.head
      buffer.add(lastKey + 1, "X")
      val evictedFirst = buffer.orderedKeys != originalKeys && !buffer.orderedKeys.contains(0) && buffer.orderedKeys.tail.reverse.head == lastKey + 1

      buffer.add(1, "X")
      val evictedRight = buffer.orderedKeys != originalKeys && !buffer.orderedKeys.contains(2) && buffer.orderedKeys.head == 1

      buffer.add(9, "X")
      val evictedRightAgain = !buffer.orderedKeys.contains(10)

      all(noEviction, evictedFirst, evictedRight, evictedRightAgain) :| "evicts records"
    }
  }

  val overwritesEntirely = forAll(maxSize) { size =>
    (size > 0) ==> {
      val buffer = LongStringBuffer(size)
      buffer.add(Range(0, size).toEmptyRecords)
      buffer.add(Range(size, size + size).toEmptyRecords)

      all(buffer.orderedKeys == Range(size, size + size).map(i => i.toLong)) :| "overwritten entirely"
    }
  }

  val overwritesSubset = forAll(maxSize) { size =>
    (size > 0) ==> {
      val buffer = LongStringBuffer(size)
      buffer.add(Range(0, size).toEmptyRecords)
      buffer.add(Range(1, size - 1).toEmptyRecords)

      all(buffer.orderedKeys == Seq(0L) ++ Range(1, size).map(i => i.toLong)) :| "overwritten with subset"
    }
  }

  val isKeyDistinct = forAll { (size: Int, singleKey: Long) =>
    (size > 0) ==> {
      val buffer = LongStringBuffer(size)
      val distinctKey = {
        for (i <- 0 to 10) buffer.add(singleKey, singleKey.toString)
        buffer.size == 1 && buffer.orderedKeys.count(_ == singleKey) == 1 && buffer.orderedValues.count(_ == singleKey.toString) == 1
      }
      all(distinctKey) :| "key-value pairs are distinct"
    }
  }

  property("add") = insertsRecords && evictsRecords && overwritesEntirely && overwritesSubset && isKeyDistinct

  val randomizedContiguousList =
    for {
      start <- Gen.Choose.chooseInt.choose(1, Int.MaxValue / 2)
      end <- Gen.Choose.chooseInt.choose(1, 1000).map(_ + start)
      length <- Gen.Choose.chooseInt.choose(1, end - start)
      gen <- Gen.pick(length, Range.inclusive(start, end).toList)
    } yield {
      gen
    }

  property("groupContiguously") = forAll(randomizedContiguousList, randomizedContiguousList, randomizedContiguousList) { (a, b, c) =>

    val groups = OverwritingBuffer.groupContiguously(a ++ b ++ c)

    val isSizeConsistent = groups.flatten.size == (a ++ b ++ c).size

    @tailrec
    def contiguous(s: Seq[Int]): Boolean = s match {
      case Nil => true
      case _ :: Nil => true
      case head :: tail => ((head + 1) == tail.head) && contiguous(tail)
    }
    val isContiguous = groups.forall(s => contiguous(s))

    all(isSizeConsistent, isContiguous)
  }

}

object OverwritingBufferTest {
  implicit class EmptyRecords(range: Range) {
    def toEmptyRecords: Map[Long, String] = range.map(_.toLong).map((_, "")).toMap
  }
}

class OverwritingBufferTest extends WordSpec {
  import OverwritingBufferTest._

  "The buffer" should {
    "not accept 0 or less as a max size" in {
      assertThrows[IllegalArgumentException](LongStringBuffer(0))
      assertThrows[IllegalArgumentException](LongStringBuffer(-1))
    }

    "evict towards the right or else the head" in {
      var buffer = LongStringBuffer(10)

      buffer.add(Range(0, 10).toEmptyRecords)
      buffer.add(Range(10, 15).toEmptyRecords)
      assert(buffer.orderedKeys == Range(5, 15), "popped heads off to make space")

      buffer.add(Range(0, 10).toEmptyRecords)
      assert(buffer.orderedKeys == Range(0, 10), "overwrote all of buffer")

      buffer = LongStringBuffer(5)
      buffer.add(Range(1, 6).toEmptyRecords)
      assert(buffer.orderedKeys == Range(1, 6))
      buffer.add(0, "X")
      assert(buffer.orderedKeys == Seq(0) ++ Range(2, 6), "overwrote buffer towards right-hand side")
    }

    "remove tuples" in {
      val buffer = LongStringBuffer(10)
      buffer.add(Range(0, 10).toEmptyRecords.map(p => p.copy(_2 = p._1.toString)))

      val taken = buffer.take(5, 2)
      assert(buffer.orderedKeys == Seq(0, 1, 2, 3, 4, 7, 8, 9))
      assert(taken == Range(5, 7).map(_.toString))

      assert(buffer.take(9, 10) == Seq("9"))
      assert(buffer.take(9, 10) == Nil)
    }
  }
}
