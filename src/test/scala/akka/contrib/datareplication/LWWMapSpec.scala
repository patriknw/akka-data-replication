/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.datareplication

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.contrib.datareplication.Replicator.Changed

class LWWMapSpec extends WordSpec with Matchers {
  import LWWRegister.defaultClock

  val node1 = UniqueAddress(Address("akka.tcp", "Sys", "localhost", 2551), 1)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2)

  "A LWWMap" must {

    "be able to set entries" in {
      val m = LWWMap().put(node1, "a", 1, defaultClock).put(node2, "b", 2, defaultClock)
      m.entries should be(Map("a" -> 1, "b" -> 2))
    }

    "be able to have its entries correctly merged with another LWWMap with other entries" in {
      val m1 = LWWMap.empty.put(node1, "a", 1, defaultClock).put(node1, "b", 2, defaultClock)
      val m2 = LWWMap.empty.put(node2, "c", 3, defaultClock)

      // merge both ways
      val expected = Map("a" -> 1, "b" -> 2, "c" -> 3)
      (m1 merge m2).entries should be(expected)
      (m2 merge m1).entries should be(expected)
    }

    "be able to remove entry" in {
      val m1 = LWWMap.empty.put(node1, "a", 1, defaultClock).put(node1, "b", 2, defaultClock)
      val m2 = LWWMap.empty.put(node2, "c", 3, defaultClock)

      val merged1 = m1 merge m2

      val m3 = merged1.remove(node1, "b")
      (merged1 merge m3).entries should be(Map("a" -> 1, "c" -> 3))

      // but if there is a conflicting update the entry is not removed
      val m4 = merged1.put(node2, "b", 22, defaultClock)
      (m3 merge m4).entries should be(Map("a" -> 1, "b" -> 22, "c" -> 3))
    }

    "have unapply extractor" in {
      val m1 = LWWMap.empty.put(node1, "a", 1L, defaultClock)
      val LWWMap(entries1) = m1
      val entries2: Map[String, Long] = entries1
      Changed("key", m1) match {
        case Changed("key", LWWMap(entries3)) =>
          val entries4: Map[String, Any] = entries3
          entries4 should be(Map("a" -> 1L))
      }
    }

  }
}
