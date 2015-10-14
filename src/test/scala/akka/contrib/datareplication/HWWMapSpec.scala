/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.datareplication

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.actor.Address
import akka.cluster.UniqueAddress

class HWWMapSpec extends WordSpec with Matchers {


  "A HWWMap" must {

    "be able to add entries" in {
      val m = HWWMap().put("a", 1).put("b", 2)
      m.get("a") should be(1)
      m.get("b") should be(2)

      val m2 = m.put("a", 3)
      m2.get("a") should be(3)
    }

    "be able to change entry" in {
      val m = HWWMap().put("a", 1).put("b", 2).put("a", 0)
      m.get("a") should be(0)
      m.get("b") should be(2)
    }

    "be able to change multiple times" in {
      val m = HWWMap().put("a", 1).put("b", 2).put("a", 0).put("a", 2).put("b", 1)
      m.get("a") should be(2)
      m.get("b") should be(1)
    }

    "be able to have its entries correctly merged with another HWWMap with other entries" in {
      val m1 = HWWMap().put("a", 1).put("b", 2)
      val m2 = HWWMap().put("c", 3)

      // merge both ways
      val merged1 = m1 merge m2
      merged1.get("a") should be(1)
      merged1.get("b") should be(2)
      merged1.get("c") should be(3)

      val merged2 = m2 merge m1
      merged2.get("a") should be(1)
      merged2.get("b") should be(2)
      merged2.get("c") should be(3)

    }

    "be able to have its entries correctly merged with another HWWMap with overlapping entries" in {

      val m1 = HWWMap().put("a", 11).put("b", 12).put("a", 0).put("d", 14)
      val m2 = HWWMap().put("c", 23).put("a", 21).put("b", 22).put("b", 0).put("d", 24)

      // a -> 0 or a -> 21 can both happen, but not a -> 11
      // b -> 12 or b -> 0 can happen, but not b -> 22
      // c -> 23 must be true
      // d -> 24 or d -> 14 can happen, but not d -> 0
      val merged1 = m1 merge m2
      merged1.get("a") should (equal(0) or equal(21))
      merged1.get("b") should (equal(0) or equal(12))
      merged1.get("c") should be(23)
      merged1.get("d") should (equal(14) or equal(24))

      val merged2 = m2 merge m1
      merged1.get("a") should be(merged2.get("a"))
      merged1.get("b") should be(merged2.get("b"))
      merged1.get("c") should be(merged2.get("c"))
      merged1.get("d") should be(merged2.get("d"))
    }

  }
}
