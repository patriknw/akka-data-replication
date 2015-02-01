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

object ReplicatorPruningSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.log-dead-letters-during-shutdown = off
    """))

}

class ReplicatorPruningSpecMultiJvmNode1 extends ReplicatorPruningSpec
class ReplicatorPruningSpecMultiJvmNode2 extends ReplicatorPruningSpec
class ReplicatorPruningSpecMultiJvmNode3 extends ReplicatorPruningSpec

class ReplicatorPruningSpec extends MultiNodeSpec(ReplicatorPruningSpec) with STMultiNodeSpec with ImplicitSender {
  import ReplicatorPruningSpec._
  import Replicator._

  override def initialParticipants = roles.size

  implicit val cluster = Cluster(system)
  val maxPruningDissemination = 3.seconds
  val replicator = system.actorOf(Replicator.props(
    ReplicatorSettings(role = None, gossipInterval = 1.second, pruningInterval = 1.second,
      maxPruningDissemination = maxPruningDissemination)), "replicator")
  val timeout = 2.seconds.dilated

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "Pruning of CRDT" must {

    "move data from removed node" in {
      join(first, first)
      join(second, first)
      join(third, first)

      within(5.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(3))
        }
      }

      // we need the UniqueAddress
      val memberProbe = TestProbe()
      cluster.subscribe(memberProbe.ref, initialStateMode = InitialStateAsEvents, classOf[MemberUp])
      val thirdUniqueAddress = {
        val member = memberProbe.fishForMessage(3.seconds) {
          case MemberUp(m) if m.address == node(third).address ⇒ true
          case _ ⇒ false
        }.asInstanceOf[MemberUp].member
        member.uniqueAddress
      }

      replicator ! Update("A", GCounter(), WriteAll(timeout))(_ + 3)
      expectMsg(UpdateSuccess("A", None))

      replicator ! Update("B", ORSet(), WriteAll(timeout))(_ + "a" + "b" + "c")
      expectMsg(UpdateSuccess("B", None))

      replicator ! Update("C", PNCounterMap(), WriteAll(timeout))(_ increment "x" increment "y")
      expectMsg(UpdateSuccess("C", None))

      enterBarrier("updates-done")

      replicator ! Get("A", ReadLocal)
      val oldCounter = expectMsgType[GetSuccess].data.asInstanceOf[GCounter]
      oldCounter.value should be(9)

      replicator ! Get("B", ReadLocal)
      val oldSet = expectMsgType[GetSuccess].data.asInstanceOf[ORSet]
      oldSet.value should be(Set("a", "b", "c"))

      replicator ! Get("C", ReadLocal)
      val oldMap = expectMsgType[GetSuccess].data.asInstanceOf[PNCounterMap]
      oldMap.get("x") should be(Some(3))
      oldMap.get("y") should be(Some(3))

      enterBarrier("get-old")

      runOn(first) {
        cluster.leave(node(third).address)
      }

      runOn(first, second) {
        within(15.seconds) {
          awaitAssert {
            replicator ! GetReplicaCount
            expectMsg(ReplicaCount(2))
          }
        }
      }
      enterBarrier("third-removed")

      runOn(first, second) {
        within(15.seconds) {
          awaitAssert {
            replicator ! Get("A", ReadLocal)
            expectMsgPF() {
              case GetSuccess(_, c: GCounter, _) ⇒
                c.value should be(9)
                c.needPruningFrom(thirdUniqueAddress) should be(false)
            }
          }
        }
        within(5.seconds) {
          awaitAssert {
            replicator ! Get("B", ReadLocal)
            expectMsgPF() {
              case GetSuccess(_, s: ORSet, _) ⇒
                s.value should be(Set("a", "b", "c"))
                s.needPruningFrom(thirdUniqueAddress) should be(false)
            }
          }
        }
        within(5.seconds) {
          awaitAssert {
            replicator ! Get("C", ReadLocal)
            expectMsgPF() {
              case GetSuccess(_, m: PNCounterMap, _) ⇒
                m.entries should be(Map("x" -> 3L, "y" -> 3L))
                m.needPruningFrom(thirdUniqueAddress) should be(false)
            }
          }
        }
      }
      enterBarrier("pruning-done")

      // on one of the nodes the data has been updated by the pruning,
      // client can update anyway
      def updateAfterPruning(expectedValue: Int): Unit = {
        replicator ! Update("A", GCounter(), WriteAll(timeout), None)(_ + 1)
        expectMsgPF() {
          case UpdateSuccess("A", _) ⇒
            replicator ! Get("A", ReadLocal)
            val retrieved = expectMsgType[GetSuccess].data.asInstanceOf[GCounter]
            retrieved.value should be(expectedValue)
        }
      }
      runOn(first) {
        updateAfterPruning(expectedValue = 10)
      }
      enterBarrier("update-first-after-pruning")

      runOn(second) {
        updateAfterPruning(expectedValue = 11)
      }
      enterBarrier("update-second-after-pruning")

      // after pruning performed and maxDissemination it is tombstoned
      // and we should still not be able to update with data from removed node
      expectNoMsg(maxPruningDissemination + 3.seconds)

      runOn(first) {
        updateAfterPruning(expectedValue = 12)
      }
      enterBarrier("update-first-after-tombstone")

      runOn(second) {
        updateAfterPruning(expectedValue = 13)
      }
      enterBarrier("update-second-after-tombstone")

      enterBarrier("after-1")
    }
  }

}

