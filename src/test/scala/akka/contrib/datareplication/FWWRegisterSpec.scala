/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.datareplication

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.contrib.datareplication.Replicator.Changed

class FWWRegisterSpec extends WordSpec with Matchers {
  import FWWRegister.defaultClock

  val node1 = UniqueAddress(Address("akka.tcp", "Sys", "localhost", 2551), 1)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2)

  "A FWWRegister" must {
    "use latest of successive assignments" in {
      val r = (1 to 100).foldLeft(FWWRegister(node1, 0, defaultClock)) {
        case (r, n) ⇒
          r.value should be(n - 1)
          r.withValue(node1, n, defaultClock)
      }
      r.value should be(100)
    }

    "merge by picking min timestamp" in {
      val clock = new FWWRegister.Clock {
        val i = Iterator.from(100)
        override def nextTimestamp(current: Long): Long = i.next()
      }
      val r1 = FWWRegister(node1, "A", clock)
      r1.timestamp should be(100)
      val r2 = r1.withValue(node2, "B", clock)
      r2.timestamp should be(101)
      val m1 = r1 merge r2
      m1.value should be("A")
      m1.timestamp should be(100)
      val m2 = r2 merge r1
      m2.value should be("A")
      m2.timestamp should be(100)
    }

    "merge by picking least address when same timestamp" in {
      val clock = new FWWRegister.Clock {
        override def nextTimestamp(current: Long): Long = 100
      }
      val r1 = FWWRegister(node1, "A", clock)
      val r2 = FWWRegister(node2, "B", clock)
      val m1 = r1 merge r2
      m1.value should be("A")
      val m2 = r2 merge r1
      m2.value should be("A")
    }

    "use monotonically increasing defaultClock" in {
      (1 to 100).foldLeft(FWWRegister(node1, 0, defaultClock)) {
        case (r, n) ⇒
          r.value should be(n - 1)
          val r2 = r.withValue(node1, n, defaultClock)
          r2.timestamp should be > r.timestamp
          r2
      }
    }

    "have unapply extractor" in {
      val r1 = FWWRegister(node1, "a", defaultClock)
      val FWWRegister(value1) = r1
      val value2: String = value1
      Changed("key", r1) match {
        case Changed("key", FWWRegister(value3)) =>
          val value4: Any = value3
          value4 should be("a")
      }
    }
  }
}
