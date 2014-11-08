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
import scala.concurrent.forkjoin.ThreadLocalRandom
import akka.actor.Actor
import akka.actor.Props
import scala.concurrent.Await
import akka.actor.ActorRef

object PerformanceSpec extends MultiNodeConfig {
  val n1 = role("n1")
  val n2 = role("n2")
  val n3 = role("n3")
  val n4 = role("n4")
  val n5 = role("n5")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = ERROR
    akka.stdout-loglevel = ERROR
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.remote.log-remote-lifecycle-events = ERROR
    akka.remote.log-frame-size-exceeding=1000b
    akka.testconductor.barrier-timeout = 60 s
    akka.contrib.data-replication.gossip-interval = 1 s
    """))

  def countDownProps(latch: TestLatch): Props = Props(new CountDown(latch))

  class CountDown(latch: TestLatch) extends Actor {
    def receive = {
      case _ =>
        latch.countDown()
        if (latch.isOpen)
          context.stop(self)
    }
  }

}

class PerformanceSpecMultiJvmNode1 extends PerformanceSpec
class PerformanceSpecMultiJvmNode2 extends PerformanceSpec
class PerformanceSpecMultiJvmNode3 extends PerformanceSpec
class PerformanceSpecMultiJvmNode4 extends PerformanceSpec
class PerformanceSpecMultiJvmNode5 extends PerformanceSpec

class PerformanceSpec extends MultiNodeSpec(PerformanceSpec) with STMultiNodeSpec with ImplicitSender {
  import PerformanceSpec._
  import Replicator._

  override def initialParticipants = roles.size

  implicit val cluster = Cluster(system)
  val replicator = DataReplication(system).replicator
  val timeout = 3.seconds.dilated
  val factor = 1
  val repeatCount = 3 // use at least 10 here for serious tuning

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  def repeat(description: String, keys: Iterable[String], n: Int, expectedAfterReplication: Option[Set[Int]] = None)(
    block: (String, Int, ActorRef) => Unit, afterEachKey: String => Unit = _ => ()): Unit = {

    keys.foreach { key =>
      val startTime = System.nanoTime()
      runOn(n1) {
        val latch = TestLatch(n)
        val replyTo = system.actorOf(countDownProps(latch))

        for (i <- 0 until n)
          block(key, i, replyTo)
        Await.ready(latch, 5.seconds * factor)
      }
      expectedAfterReplication.foreach { expected =>
        enterBarrier("repeat-" + key + "-before-awaitReplicated")
        awaitReplicated(key, expected)
        enterBarrier("repeat-" + key + "-after-awaitReplicated")
      }
      runOn(n1) {
        val endTime = System.nanoTime()
        val durationMs = (endTime - startTime).nanos.toMillis
        val tps = (n * 1000.0 / durationMs).toInt
        println(s"## $n $description took $durationMs ms, $tps TPS")
      }

      afterEachKey(key)
      enterBarrier("repeat-" + key + "-done")
    }
  }

  def awaitReplicated(keys: Iterable[String], expectedData: Set[Int]): Unit =
    keys.foreach { key => awaitReplicated(key, expectedData) }

  def awaitReplicated(key: String, expectedData: Set[Int]): Unit = {
    within(20.seconds) {
      awaitAssert {
        val readProbe = TestProbe()
        replicator.tell(Get(key, ReadLocal), readProbe.ref)
        val result = readProbe.expectMsgPF() { case GetSuccess(key, set: ORSet, _) ⇒ set }
        result.value should be(expectedData)
      }
    }
  }

  "Performance" must {

    "setup cluster" in {
      roles.foreach { join(_, n1) }

      within(10.seconds) {
        awaitAssert {
          replicator ! Internal.GetNodeCount
          expectMsg(Internal.NodeCount(roles.size))
        }
      }

      enterBarrier("after-setup")
    }

    "be great for ORSet Update WriteLocal" in {
      val keys = (1 to repeatCount).map("A" + _)
      val n = 500 * factor
      val expectedData = (0 until n).toSet
      repeat("ORSet Update WriteLocal", keys, n)({ (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), WriteLocal)(_ + i), replyTo)
      }, key => awaitReplicated(key, expectedData))

      enterBarrier("after-1")
    }

    "be blazingly fast for ORSet Get ReadLocal" in {
      val keys = (1 to repeatCount).map("A" + _)
      repeat("Get ReadLocal", keys, 1000000 * factor) { (key, i, replyTo) =>
        replicator.tell(Get(key, ReadLocal), replyTo)
      }
      enterBarrier("after-2")
    }

    "be good for ORSet Update WriteLocal and gossip replication" in {
      val keys = (1 to repeatCount).map("B" + _)
      val n = 500 * factor
      val expected = Some((0 until n).toSet)
      repeat("ORSet Update WriteLocal + gossip", keys, n, expected) { (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), WriteLocal)(_ + i), replyTo)
      }
      enterBarrier("after-3")
    }

    "be good for ORSet Update WriteLocal and gossip of existing keys" in {
      val keys = (1 to repeatCount).map("B" + _)
      val n = 500 * factor
      val expected = Some((0 until n).toSet ++ (0 until n).map(-_).toSet)
      repeat("ORSet Update WriteLocal existing + gossip", keys, n, expected) { (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), WriteLocal)(_ + (-i)), replyTo)
      }
      enterBarrier("after-4")
    }

    "be good for ORSet Update WriteTwo and gossip replication" in {
      val keys = (1 to repeatCount).map("C" + _)
      val n = 500 * factor
      val expected = Some((0 until n).toSet)
      val writeTwo = WriteTo(2, timeout)
      repeat("ORSet Update WriteTwo + gossip", keys, n, expected) { (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), writeTwo)(_ + i), replyTo)
      }
      enterBarrier("after-5")
    }

    "be awesome for GCounter Update WriteLocal" in {
      val startTime = System.nanoTime()
      val n = 100000 * factor
      val key = "D"
      runOn(n1, n2, n3) {
        val latch = TestLatch(n)
        val replyTo = system.actorOf(countDownProps(latch))
        for (_ <- 0 until n)
          replicator.tell(Update(key, GCounter(), WriteLocal)(_ + 1), replyTo)
        Await.ready(latch, 5.seconds * factor)
        enterBarrier("update-done-6")
        runOn(n1) {
          val endTime = System.nanoTime()
          val durationMs = (endTime - startTime).nanos.toMillis
          val tps = (3 * n * 1000.0 / durationMs).toInt
          println(s"## ${3 * n} GCounter Update took $durationMs ms, $tps TPS")
        }
      }
      runOn(n4, n5) {
        enterBarrier("update-done-6")
      }

      within(20.seconds) {
        awaitAssert {
          val readProbe = TestProbe()
          replicator.tell(Get(key, ReadLocal), readProbe.ref)
          val result = readProbe.expectMsgPF() { case GetSuccess(key, c: GCounter, _) ⇒ c }
          result.value should be(3 * n)
        }
      }
      enterBarrier("replication-done-6")
      runOn(n1) {
        val endTime = System.nanoTime()
        val durationMs = (endTime - startTime).nanos.toMillis
        val tps = (n * 1000.0 / durationMs).toInt
        println(s"## $n GCounter Update + gossip took $durationMs ms, $tps TPS")
      }

      enterBarrier("after-6")
    }

  }

}

