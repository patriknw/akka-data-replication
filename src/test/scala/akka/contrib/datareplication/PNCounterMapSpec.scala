/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.datareplication

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.actor.Address
import akka.cluster.UniqueAddress

class PNCounterMapSpec extends WordSpec with Matchers {

  val node1 = UniqueAddress(Address("akka.tcp", "Sys", "localhost", 2551), 1)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2)

  "A PNCounterMap" must {

    "be able to increment and decrement entries" in {
      val m = PNCounterMap().increment(node1, "a", 2).increment(node1, "b", 3).decrement(node2, "a", 1)
      m.entries should be(Map("a" -> 1, "b" -> 3))
    }

    "be able to have its entries correctly merged with another ORMap with other entries" in {
      val m1 = PNCounterMap().increment(node1, "a", 1).increment(node1, "b", 3).increment(node1, "c", 2)
      val m2 = PNCounterMap().increment(node2, "c", 5)

      // merge both ways
      val expected = Map("a" -> 1, "b" -> 3, "c" -> 7)
      (m1 merge m2).entries should be(expected)
      (m2 merge m1).entries should be(expected)
    }

    "be able to remove entry" in {
      val m1 = PNCounterMap().increment(node1, "a", 1).increment(node1, "b", 3).increment(node1, "c", 2)
      val m2 = PNCounterMap().increment(node2, "c", 5)

      val merged1 = m1 merge m2

      val m3 = merged1.remove(node1, "b")
      (merged1 merge m3).entries should be(Map("a" -> 1, "c" -> 7))

      // but if there is a conflicting update the entry is not removed
      val m4 = merged1.increment(node2, "b", 10)
      (m3 merge m4).entries should be(Map("a" -> 1, "b" -> 13, "c" -> 7))
    }

  }
}
