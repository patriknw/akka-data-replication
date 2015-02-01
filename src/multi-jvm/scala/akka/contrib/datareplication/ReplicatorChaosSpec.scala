/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import scala.concurrent.duration._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import com.typesafe.config.ConfigFactory
import akka.cluster.Cluster
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit._
import akka.cluster.ClusterEvent.InitialStateAsEvents
import akka.cluster.ClusterEvent.MemberUp

object ReplicatorChaosSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.cluster.roles = ["backend"]
    akka.log-dead-letters-during-shutdown = off
    """))

  testTransport(on = true)
}

class ReplicatorChaosSpecMultiJvmNode1 extends ReplicatorChaosSpec
class ReplicatorChaosSpecMultiJvmNode2 extends ReplicatorChaosSpec
class ReplicatorChaosSpecMultiJvmNode3 extends ReplicatorChaosSpec
class ReplicatorChaosSpecMultiJvmNode4 extends ReplicatorChaosSpec
class ReplicatorChaosSpecMultiJvmNode5 extends ReplicatorChaosSpec

class ReplicatorChaosSpec extends MultiNodeSpec(ReplicatorChaosSpec) with STMultiNodeSpec with ImplicitSender {
  import ReplicatorChaosSpec._
  import Replicator._

  override def initialParticipants = roles.size

  implicit val cluster = Cluster(system)
  val replicator = system.actorOf(Replicator.props(
    ReplicatorSettings(role = Some("backend"), gossipInterval = 1.second)), "replicator")
  val timeout = 3.seconds.dilated

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  def assertValue(key: String, expected: Any): Unit =
    within(10.seconds) {
      awaitAssert {
        replicator ! Get(key, ReadLocal)
        val value = expectMsgPF() {
          case GetSuccess(`key`, c: GCounter, _)  ⇒ c.value
          case GetSuccess(`key`, c: PNCounter, _) ⇒ c.value
          case GetSuccess(`key`, c: GSet, _)      ⇒ c.value
          case GetSuccess(`key`, c: ORSet, _)     ⇒ c.value
        }
        value should be(expected)
      }
    }

  def assertDeleted(key: String): Unit =
    within(5.seconds) {
      awaitAssert {
        replicator ! Get(key, ReadLocal)
        expectMsg(DataDeleted(key))
      }
    }

  "Replicator in chaotic cluster" must {

    "replicate data in initial phase" in {
      join(first, first)
      join(second, first)
      join(third, first)
      join(fourth, first)
      join(fifth, first)

      within(10.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(5))
        }
      }

      runOn(first) {
        (0 until 5).foreach { i ⇒
          replicator ! Update("A", GCounter(), WriteLocal)(_ + 1)
          replicator ! Update("B", PNCounter(), WriteLocal)(_ - 1)
          replicator ! Update("C", GCounter(), WriteAll(timeout))(_ + 1)
        }
        receiveN(15).map(_.getClass).toSet should be(Set(classOf[UpdateSuccess]))
      }

      runOn(second) {
        replicator ! Update("A", GCounter(), WriteLocal)(_ + 20)
        replicator ! Update("B", PNCounter(), WriteTo(2, timeout))(_ + 20)
        replicator ! Update("C", GCounter(), WriteAll(timeout))(_ + 20)
        receiveN(3).toSet should be(Set(UpdateSuccess("A", None),
          UpdateSuccess("B", None), UpdateSuccess("C", None)))

        replicator ! Update("E", GSet(), WriteLocal)(_ + "e1" + "e2")
        expectMsg(UpdateSuccess("E", None))

        replicator ! Update("F", ORSet(), WriteLocal)(_ + "e1" + "e2")
        expectMsg(UpdateSuccess("F", None))
      }

      runOn(fourth) {
        replicator ! Update("D", GCounter(), WriteLocal)(_ + 40)
        expectMsg(UpdateSuccess("D", None))

        replicator ! Update("E", GSet(), WriteLocal)(_ + "e2" + "e3")
        expectMsg(UpdateSuccess("E", None))

        replicator ! Update("F", ORSet(), WriteLocal)(_ + "e2" + "e3")
        expectMsg(UpdateSuccess("F", None))
      }

      runOn(fifth) {
        replicator ! Update("X", GCounter(), WriteTo(2, timeout))(_ + 50)
        expectMsg(UpdateSuccess("X", None))
        replicator ! Delete("X", WriteLocal)
        expectMsg(DeleteSuccess("X"))
      }

      enterBarrier("initial-updates-done")

      assertValue("A", 25)
      assertValue("B", 15)
      assertValue("C", 25)
      assertValue("D", 40)
      assertValue("E", Set("e1", "e2", "e3"))
      assertValue("F", Set("e1", "e2", "e3"))
      assertDeleted("X")

      enterBarrier("after-1")
    }

    "be available during network split" in {
      val side1 = Seq(first, second)
      val side2 = Seq(third, fourth, fifth)
      runOn(first) {
        for (a ← side1; b ← side2)
          testConductor.blackhole(a, b, Direction.Both).await
      }
      enterBarrier("split")

      runOn(first) {
        replicator ! Update("A", GCounter(), WriteTo(2, timeout))(_ + 1)
        expectMsg(UpdateSuccess("A", None))
      }

      runOn(third) {
        replicator ! Update("A", GCounter(), WriteTo(2, timeout))(_ + 2)
        expectMsg(UpdateSuccess("A", None))

        replicator ! Update("E", GSet(), WriteTo(2, timeout))(_ + "e4")
        expectMsg(UpdateSuccess("E", None))

        replicator ! Update("F", ORSet(), WriteTo(2, timeout))(_ - "e2")
        expectMsg(UpdateSuccess("F", None))
      }
      runOn(fourth) {
        replicator ! Update("D", GCounter(), WriteTo(2, timeout))(_ + 1)
        expectMsg(UpdateSuccess("D", None))
      }
      enterBarrier("update-during-split")

      runOn(side1: _*) {
        assertValue("A", 26)
        assertValue("B", 15)
        assertValue("D", 40)
        assertValue("E", Set("e1", "e2", "e3"))
        assertValue("F", Set("e1", "e2", "e3"))
      }
      runOn(side2: _*) {
        assertValue("A", 27)
        assertValue("B", 15)
        assertValue("D", 41)
        assertValue("E", Set("e1", "e2", "e3", "e4"))
        assertValue("F", Set("e1", "e3"))
      }
      enterBarrier("update-during-split-verified")

      runOn(first) {
        testConductor.exit(fourth, 0).await
      }

      enterBarrier("after-2")
    }

    "converge after partition" in {
      val side1 = Seq(first, second)
      val side2 = Seq(third, fifth) // fourth was shutdown
      runOn(first) {
        for (a ← side1; b ← side2)
          testConductor.passThrough(a, b, Direction.Both).await
      }
      enterBarrier("split-repaired")

      assertValue("A", 28)
      assertValue("B", 15)
      assertValue("C", 25)
      assertValue("D", 41)
      assertValue("E", Set("e1", "e2", "e3", "e4"))
      assertValue("F", Set("e1", "e3"))
      assertDeleted("X")

      enterBarrier("after-3")
    }
  }

}

