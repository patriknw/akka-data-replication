/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.datareplication

import org.scalatest.WordSpec
import org.scalatest.Matchers
import scala.collection.immutable.TreeMap
import akka.actor.Address
import akka.cluster.UniqueAddress

class HWWSetSpec extends WordSpec with Matchers {

  val user1 = """{"username":"john","password":"coltrane"}"""
  val user2 = """{"username":"sonny","password":"rollins"}"""
  val user3 = """{"username":"charlie","password":"parker"}"""
  val user4 = """{"username":"charles","password":"mingus"}"""

  "A HWWSet" must {

    "be able to add user" in {
      val c1 = HWWSet()

      val c2 = c1.add(user1)
      val c3 = c2.add(user2)

      val c4 = c3.add(user4)
      val c5 = c4.add(user3)

      c5.value should contain(user1)
      c5.value should contain(user2)
      c5.value should contain(user3)
      c5.value should contain(user4)
    }

    "be able to remove added user" in {
      val c1 = HWWSet()

      val c2 = c1.add(user1)
      val c3 = c2.add(user2)

      val c4 = c3.remove(user2)
      val c5 = c4.remove(user1)

      c5.value should not contain (user1)
      c5.value should not contain (user2)
    }

    "be able to add removed" in {
      val c1 = HWWSet()
      val c2 = c1.remove(user1)
      val c3 = c2.add(user1)
      c3.value should contain(user1)
      val c4 = c3.remove(user1)
      c4.value should not contain (user1)
      val c5 = c4.add(user1)
      c5.value should contain(user1)
    }

    "be able to remove and add several times" in {
      val c1 = HWWSet()

      val c2 = c1.add(user1)
      val c3 = c2.add(user2)
      val c4 = c3.remove(user1)
      c4.value should not contain (user1)
      c4.value should contain(user2)

      val c5 = c4.add(user1)
      val c6 = c5.add(user2)
      c6.value should contain(user1)
      c6.value should contain(user2)

      val c7 = c6.remove(user1)
      val c8 = c7.add(user2)
      val c9 = c8.remove(user1)
      c9.value should not contain (user1)
      c9.value should contain(user2)
    }

    "be able to have its user set correctly merged with another HWWSet with unique user sets" in {
      // set 1
      val c1 = HWWSet().add(user1).add(user2)
      c1.value should contain(user1)
      c1.value should contain(user2)

      // set 2
      val c2 = HWWSet().add(user3).add(user4).remove(user3)

      c2.value should not contain (user3)
      c2.value should contain(user4)

      println(c1)
      println(c2)

      // merge both ways
      println("MERGING")
      val merged1 = c1 merge c2
      merged1.value should contain(user1)
      merged1.value should contain(user2)
      merged1.value should not contain (user3)
      merged1.value should contain(user4)

      val merged2 = c2 merge c1
      merged2.value should contain(user1)
      merged2.value should contain(user2)
      merged2.value should not contain (user3)
      merged2.value should contain(user4)
    }

    "be able to have its user set correctly merged with another HWWSet with overlapping user sets" in {
      // set 1
      val c1 = HWWSet().add(user1).add(user2).add(user3).remove(user1).remove(user3)

      c1.value should not contain (user1)
      c1.value should contain(user2)
      c1.value should not contain (user3)

      // set 2
      val c2 = HWWSet().add(user1).add(user2).add(user3).add(user4).remove(user3)

      c2.value should contain(user1)
      c2.value should contain(user2)
      c2.value should not contain (user3)
      c2.value should contain(user4)

      // merge both ways
      val merged1 = c1 merge c2
      // Difference from ORSet, since c1 only added user1, while c2 added and already removed
      // it, so c2 wins by having the longest write chain on the user1 entry
      // Observe that this corresponds to the following possible serialization:
      // .add(user1)           .add(user2)           .add(user3)                      .remove(user1).remove(user3)
      //            .add(user1)           .add(user2)           .add(user3).add(user4)                            .remove(user3)
      merged1.value should not contain (user1)
      merged1.value should contain(user2)
      merged1.value should not contain (user3)
      merged1.value should contain(user4)

      val merged2 = c2 merge c1
      // Difference from ORSet, since c1 only added user1, while c2 added and already removed
      // it, so c2 wins by having the longest write chain on the user1 entry
      // Observe that this corresponds to the following possible serialization:
      // .add(user1)           .add(user2)           .add(user3)                      .remove(user1).remove(user3)
      //            .add(user1)           .add(user2)           .add(user3).add(user4)                            .remove(user3)
      merged2.value should not contain(user1)
      merged2.value should contain(user2)
      merged2.value should not contain (user3)
      merged2.value should contain(user4)
    }

    "be able to have its user set correctly merged for concurrent updates" in {
      val c1 = HWWSet().add(user1).add(user2).add(user3)

      c1.value should contain(user1)
      c1.value should contain(user2)
      c1.value should contain(user3)

      val c2 = c1.add(user1).remove(user2).remove(user3)

      c2.value should contain(user1)
      c2.value should not contain (user2)
      c2.value should not contain (user3)

      // merge both ways
      val merged1 = c1 merge c2
      merged1.value should contain(user1)
      merged1.value should not contain (user2)
      merged1.value should not contain (user3)

      val merged2 = c2 merge c1
      merged2.value should contain(user1)
      merged2.value should not contain (user2)
      merged2.value should not contain (user3)

      val c3 = c1.add(user4).remove(user3).add(user2)

      // merge both ways
      val merged3 = c2 merge c3
      merged3.value should contain(user1)
      // Difference from ORSet, since c1.add(user2) is treated as NOP since it does not contribute
      // to the length of the local write chain
      // Observe that this corresponds to the following possible serialization (starting from state (user1, user2, user3)):
      // .add(user1)                      .remove(user2).remove(user3)
      //            .add(user2).add(user4)                            .remove(user3)
      // Notice that add(user2) was moved to the front for the second writer -- in HWWSet no ordering is defined between
      // operations on different element, so an .op(x).op(y) might be "reordered" to an .op(y).op(x)
      merged3.value should not contain (user2)
      merged3.value should not contain (user3)
      merged3.value should contain(user4)

      val merged4 = c3 merge c2
      merged4.value should contain(user1)
      // Difference from ORSet, since c1.add(user2) is treated as NOP since it does not contribute
      // to the length of the local write chain
      // Observe that this corresponds to the following possible serialization (starting from state (user1, user2, user3)):
      // .add(user1)                      .remove(user2).remove(user3)
      //            .add(user2).add(user4)                            .remove(user3)
      // Notice that add(user2) was moved to the front for the second writer -- in HWWSet no ordering is defined between
      // operations on different element, so an .op(x).op(y) might be "reordered" to an .op(y).op(x)
      merged4.value should not contain (user2)
      merged4.value should not contain (user3)
      merged4.value should contain(user4)
    }

    "be able to have its user set correctly merged after remove" in {
      val c1 = HWWSet().add(user1).add(user2)
      val c2 = c1.remove(user2)

      // merge both ways
      val merged1 = c1 merge c2
      merged1.value should contain(user1)
      merged1.value should not contain (user2)

      val merged2 = c2 merge c1
      merged2.value should contain(user1)
      merged2.value should not contain (user2)

      val c3 = c1.add(user3)

      // merge both ways
      val merged3 = c3 merge c2
      merged3.value should contain(user1)
      merged3.value should not contain (user2)
      merged3.value should contain(user3)

      val merged4 = c2 merge c3
      merged4.value should contain(user1)
      merged4.value should not contain (user2)
      merged4.value should contain(user3)
    }

  }


}
