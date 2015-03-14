/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
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

object ReplicatorSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.log-dead-letters-during-shutdown = off
    """))

  testTransport(on = true)

}

class ReplicatorSpecMultiJvmNode1 extends ReplicatorSpec
class ReplicatorSpecMultiJvmNode2 extends ReplicatorSpec
class ReplicatorSpecMultiJvmNode3 extends ReplicatorSpec

class ReplicatorSpec extends MultiNodeSpec(ReplicatorSpec) with STMultiNodeSpec with ImplicitSender {
  import ReplicatorSpec._
  import Replicator._

  override def initialParticipants = roles.size

  implicit val cluster = Cluster(system)
  val replicator = system.actorOf(Replicator.props(
    ReplicatorSettings(role = None, gossipInterval = 1.second, maxDeltaElements = 10)), "replicator")
  val timeout = 2.seconds.dilated
  val writeTwo = WriteTo(2, timeout)
  val writeQuorum = WriteQuorum(timeout)
  val writeAll = WriteAll(timeout)
  val readTwo = ReadFrom(2, timeout)
  val readAll = ReadAll(timeout)
  val readQuorum = ReadQuorum(timeout)

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "Cluster CRDT" must {

    "work in single node cluster" in {
      join(first, first)

      runOn(first) {

        within(5.seconds) {
          awaitAssert {
            replicator ! GetReplicaCount
            expectMsg(ReplicaCount(1))
          }
        }

        val changedProbe = TestProbe()
        replicator ! Subscribe("A", changedProbe.ref)
        replicator ! Subscribe("X", changedProbe.ref)

        replicator ! Get("A", ReadLocal)
        expectMsg(NotFound("A", None))

        val c3 = GCounter() + 3
        replicator ! Update("A", GCounter(), WriteLocal)(_ + 3)
        expectMsg(UpdateSuccess("A", None))
        replicator ! Get("A", ReadLocal)
        expectMsg(GetSuccess("A", c3, None))
        changedProbe.expectMsg(Changed("A", c3))

        val changedProbe2 = TestProbe()
        replicator ! Subscribe("A", changedProbe2.ref)
        changedProbe2.expectMsg(Changed("A", c3))

        val c4 = c3 + 1
        // too strong consistency level
        replicator ! Update("A", GCounter(), writeTwo)(_ + 1)
        expectMsg(UpdateTimeout("A", None))
        replicator ! Get("A", ReadLocal)
        expectMsg(GetSuccess("A", c4, None))
        changedProbe.expectMsg(Changed("A", c4))

        val c5 = c4 + 1
        // too strong consistency level
        replicator ! Update("A", GCounter(), writeQuorum)(_ + 1)
        expectMsg(UpdateSuccess("A", None))
        replicator ! Get("A", readQuorum)
        expectMsg(GetSuccess("A", c5, None))
        changedProbe.expectMsg(Changed("A", c5))

        val c6 = c5 + 1
        replicator ! Update("A", GCounter(), writeAll)(_ + 1)
        expectMsg(UpdateSuccess("A", None))
        replicator ! Get("A", readAll)
        expectMsg(GetSuccess("A", c6, None))
        changedProbe.expectMsg(Changed("A", c6))

        val c9 = GCounter() + 9
        replicator ! Update("X", GCounter(), WriteLocal)(_ + 9)
        expectMsg(UpdateSuccess("X", None))
        changedProbe.expectMsg(Changed("X", c9))
        replicator ! Delete("X", WriteLocal)
        expectMsg(DeleteSuccess("X"))
        changedProbe.expectMsg(DataDeleted("X"))
        replicator ! Get("X", ReadLocal)
        expectMsg(DataDeleted("X"))
        replicator ! Get("X", readAll)
        expectMsg(DataDeleted("X"))
        replicator ! Update("X", GCounter(), WriteLocal)(_ + 1)
        expectMsg(DataDeleted("X"))
        replicator ! Delete("X", WriteLocal)
        expectMsg(DataDeleted("X"))

        replicator ! GetKeys
        expectMsg(GetKeysResult(Set("A")))
      }

      enterBarrier("after-1")
    }
  }

  "reply with ModifyFailure if exception is thrown by modify function" in {
    val e = new RuntimeException("errr")
    replicator ! Update("A", GCounter(), WriteLocal)(_ => throw e)
    expectMsgType[ModifyFailure].cause should be(e)
  }

  "replicate values to new node" in {
    join(second, first)

    runOn(first, second) {
      within(10.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(2))
        }
      }
    }

    enterBarrier("2-nodes")

    runOn(second) {
      val changedProbe = TestProbe()
      replicator ! Subscribe("A", changedProbe.ref)
      // "A" should be replicated via gossip to the new node
      within(5.seconds) {
        awaitAssert {
          replicator ! Get("A", ReadLocal)
          val c = expectMsgPF() { case GetSuccess("A", c: GCounter, _) ⇒ c }
          c.value should be(6)
        }
      }
      val c = changedProbe.expectMsgPF() { case Changed("A", c: GCounter) ⇒ c }
      c.value should be(6)
    }

    enterBarrier("after-2")
  }

  "work in 2 node cluster" in {

    runOn(first, second) {
      // start with 20 on both nodes
      replicator ! Update("B", GCounter(), WriteLocal)(_ + 20)
      expectMsg(UpdateSuccess("B", None))

      // add 1 on both nodes using WriteTwo
      replicator ! Update("B", GCounter(), writeTwo)(_ + 1)
      expectMsg(UpdateSuccess("B", None))

      // the total, after replication should be 42
      awaitAssert {
        replicator ! Get("B", readTwo)
        val c = expectMsgPF() { case GetSuccess("B", c: GCounter, _) ⇒ c }
        c.value should be(42)
      }
    }
    enterBarrier("update-42")

    runOn(first, second) {
      // add 1 on both nodes using WriteAll
      replicator ! Update("B", GCounter(), writeAll)(_ + 1)
      expectMsg(UpdateSuccess("B", None))

      // the total, after replication should be 44
      awaitAssert {
        replicator ! Get("B", readAll)
        val c = expectMsgPF() { case GetSuccess("B", c: GCounter, _) ⇒ c }
        c.value should be(44)
      }
    }
    enterBarrier("update-44")

    runOn(first, second) {
      // add 1 on both nodes using WriteQuorum
      replicator ! Update("B", GCounter(), writeQuorum)(_ + 1)
      expectMsg(UpdateSuccess("B", None))

      // the total, after replication should be 46
      awaitAssert {
        replicator ! Get("B", readQuorum)
        val c = expectMsgPF() { case GetSuccess("B", c: GCounter, _) ⇒ c }
        c.value should be(46)
      }
    }

    enterBarrier("after-3")
  }

  "be replicated after succesful update" in {
    val changedProbe = TestProbe()
    runOn(first, second) {
      replicator ! Subscribe("C", changedProbe.ref)
    }

    runOn(first) {
      replicator ! Update("C", GCounter(), writeTwo)(_ + 30)
      expectMsg(UpdateSuccess("C", None))
      changedProbe.expectMsgPF() { case Changed("C", c: GCounter) ⇒ c.value } should be(30)

      replicator ! Update("Y", GCounter(), writeTwo)(_ + 30)
      expectMsg(UpdateSuccess("Y", None))

      replicator ! Update("Z", GCounter(), writeQuorum)(_ + 30)
      expectMsg(UpdateSuccess("Z", None))
    }
    enterBarrier("update-c30")

    runOn(second) {
      replicator ! Get("C", ReadLocal)
      val c30 = expectMsgPF() { case GetSuccess("C", c: GCounter, _) ⇒ c }
      c30.value should be(30)
      changedProbe.expectMsgPF() { case Changed("C", c: GCounter) ⇒ c.value } should be(30)

      // replicate with gossip after WriteLocal
      replicator ! Update("C", GCounter(), WriteLocal)(_ + 1)
      expectMsg(UpdateSuccess("C", None))
      changedProbe.expectMsgPF() { case Changed("C", c: GCounter) ⇒ c.value } should be(31)

      replicator ! Delete("Y", WriteLocal)
      expectMsg(DeleteSuccess("Y"))

      replicator ! Get("Z", readQuorum)
      expectMsgPF() { case GetSuccess("Z", c: GCounter, _) ⇒ c.value } should be(30)
    }
    enterBarrier("update-c31")

    runOn(first) {
      // "C" and deleted "Y" should be replicated via gossip to the other node
      within(5.seconds) {
        awaitAssert {
          replicator ! Get("C", ReadLocal)
          val c = expectMsgPF() { case GetSuccess("C", c: GCounter, _) ⇒ c }
          c.value should be(31)

          replicator ! Get("Y", ReadLocal)
          expectMsg(DataDeleted("Y"))
        }
      }
      changedProbe.expectMsgPF() { case Changed("C", c: GCounter) ⇒ c.value } should be(31)
    }
    enterBarrier("verified-c31")

    // and also for concurrent updates
    runOn(first, second) {
      replicator ! Get("C", ReadLocal)
      val c31 = expectMsgPF() { case GetSuccess("C", c: GCounter, _) ⇒ c }
      c31.value should be(31)

      val c32 = c31 + 1
      replicator ! Update("C", GCounter(), WriteLocal)(_ + 1)
      expectMsg(UpdateSuccess("C", None))

      within(5.seconds) {
        awaitAssert {
          replicator ! Get("C", ReadLocal)
          val c = expectMsgPF() { case GetSuccess("C", c: GCounter, _) ⇒ c }
          c.value should be(33)
        }
      }
    }

    enterBarrier("after-4")
  }

  "converge after partition" in {
    runOn(first) {
      replicator ! Update("D", GCounter(), writeTwo)(_ + 40)
      expectMsg(UpdateSuccess("D", None))

      testConductor.blackhole(first, second, Direction.Both).await
    }
    enterBarrier("blackhole-first-second")

    runOn(first, second) {
      replicator ! Get("D", ReadLocal)
      val c40 = expectMsgPF() { case GetSuccess("D", c: GCounter, _) ⇒ c }
      c40.value should be(40)
      replicator ! Update("D", GCounter() + 1, writeTwo)(_ + 1)
      expectMsg(UpdateTimeout("D", None))
      replicator ! Update("D", GCounter(), writeTwo)(_ + 1)
      expectMsg(UpdateTimeout("D", None))
    }
    runOn(first) {
      for (n ← 1 to 30) {
        replicator ! Update("D" + n, GCounter(), WriteLocal)(_ + n)
        expectMsg(UpdateSuccess("D" + n, None))
      }
    }
    enterBarrier("updates-during-partion")

    runOn(first) {
      testConductor.passThrough(first, second, Direction.Both).await
    }
    enterBarrier("passThrough-first-second")

    runOn(first, second) {
      replicator ! Get("D", readTwo)
      val c44 = expectMsgPF() { case GetSuccess("D", c: GCounter, _) ⇒ c }
      c44.value should be(44)

      within(10.seconds) {
        awaitAssert {
          for (n ← 1 to 30) {
            val Key = "D" + n
            replicator ! Get(Key, ReadLocal)
            expectMsgPF() { case GetSuccess(Key, c: GCounter, _) ⇒ c }.value should be(n)
          }
        }
      }
    }

    enterBarrier("after-5")
  }

  "support quorum write and read with 3 nodes with 1 unreachable" in {
    join(third, first)

    runOn(first, second, third) {
      within(10.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(3))
        }
      }
    }
    enterBarrier("3-nodes")

    runOn(first, second, third) {
      replicator ! Update("E", GCounter(), writeQuorum)(_ + 50)
      expectMsg(UpdateSuccess("E", None))
    }
    enterBarrier("write-inital-quorum")

    runOn(first, second, third) {
      replicator ! Get("E", readQuorum)
      val c150 = expectMsgPF() { case GetSuccess("E", c: GCounter, _) ⇒ c }
      c150.value should be(150)
    }
    enterBarrier("read-inital-quorum")

    runOn(first) {
      testConductor.blackhole(first, third, Direction.Both).await
      testConductor.blackhole(second, third, Direction.Both).await
    }
    enterBarrier("blackhole-third")

    runOn(second) {
      replicator ! Update("E", GCounter(), WriteLocal)(_ + 1)
      expectMsg(UpdateSuccess("E", None))
    }
    enterBarrier("local-update-from-second")

    runOn(first) {
      // ReadQuorum should retrive the previous update from second, before applying the modification
      val probe1 = TestProbe()
      val probe2 = TestProbe()
      replicator.tell(Update("E", GCounter(), readQuorum, writeQuorum, None) { data =>
        probe1.ref ! data.value
        data + 1
      }, probe2.ref)
      // verify read your own writes, without waiting for the UpdateSuccess reply
      // note that the order of the replies are not defined, and therefore we use separate probes
      val probe3 = TestProbe()
      replicator.tell(Get("E", readQuorum), probe3.ref)
      probe1.expectMsg(151)
      probe2.expectMsg(UpdateSuccess("E", None))
      val c152 = probe3.expectMsgPF() { case GetSuccess("E", c: GCounter, _) ⇒ c }
      c152.value should be(152)
    }
    enterBarrier("quorum-update-from-first")

    runOn(second) {
      val probe1 = TestProbe()
      replicator.tell(Update("E", GCounter(), readQuorum, writeQuorum, Some(153))(_ + 1), probe1.ref)
      // verify read your own writes, without waiting for the UpdateSuccess reply
      // note that the order of the replies are not defined, and therefore we use separate probes
      val probe2 = TestProbe()
      replicator.tell(Update("E", GCounter(), ReadLocal, writeQuorum, Some(154))(_ + 1), probe2.ref)
      val probe3 = TestProbe()
      replicator.tell(Update("E", GCounter(), readQuorum, writeQuorum, Some(155))(_ + 1), probe3.ref)
      val probe5 = TestProbe()
      replicator.tell(Get("E", readQuorum), probe5.ref)
      probe1.expectMsg(UpdateSuccess("E", Some(153)))
      probe2.expectMsg(UpdateSuccess("E", Some(154)))
      probe3.expectMsg(UpdateSuccess("E", Some(155)))
      val c155 = probe5.expectMsgPF() { case GetSuccess("E", c: GCounter, _) ⇒ c }
      c155.value should be(155)
    }
    enterBarrier("quorum-update-from-second")

    runOn(first, second) {
      replicator ! Update("E2", GCounter(), readAll, writeAll, Some(999))(_ + 1)
      expectMsg(ReadFailure("E2", Some(999)))
      replicator ! Get("E2", ReadLocal)
      expectMsg(NotFound("E2", None))
    }
    enterBarrier("read-all-fail-update")

    runOn(first) {
      testConductor.passThrough(first, third, Direction.Both).await
      testConductor.passThrough(second, third, Direction.Both).await
    }
    enterBarrier("passThrough-third")

    runOn(third) {
      replicator ! Get("E", readQuorum)
      val c155 = expectMsgPF() { case GetSuccess("E", c: GCounter, _) ⇒ c }
      c155.value should be(155)
    }

    enterBarrier("after-6")
  }

  "converge after many concurrent updates" in within(10.seconds) {
    runOn(first, second, third) {
      var c = GCounter()
      for (i ← 0 until 100) {
        c += 1
        replicator ! Update("F", GCounter(), writeTwo)(_ + 1)
      }
      val results = receiveN(100)
      results.map(_.getClass).toSet should be(Set(classOf[UpdateSuccess]))
    }
    enterBarrier("100-updates-done")
    runOn(first, second, third) {
      replicator ! Get("F", readTwo)
      val c = expectMsgPF() { case GetSuccess("F", c: GCounter, _) ⇒ c }
      c.value should be(3 * 100)
    }
    enterBarrier("after-7")
  }

  "read-repair happens before GetSuccess" in {
    runOn(first) {
      replicator ! Update("G", ORSet(), writeTwo)(_ + "a" + "b")
      expectMsgType[UpdateSuccess]
    }
    enterBarrier("a-b-added-to-G")
    runOn(second) {
      replicator ! Get("G", readAll)
      expectMsgPF() { case GetSuccess("G", ORSet(elements), _) ⇒ elements } should be(Set("a", "b"))
      replicator ! Get("G", ReadLocal)
      expectMsgPF() { case GetSuccess("G", ORSet(elements), _) ⇒ elements } should be(Set("a", "b"))
    }
    enterBarrier("after-8")
  }

}

