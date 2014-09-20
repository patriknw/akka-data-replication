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

object JepsenInspiredInsertSpec extends MultiNodeConfig {
  val controller = role("controller")
  val n1 = role("n1")
  val n2 = role("n2")
  val n3 = role("n3")
  val n4 = role("n4")
  val n5 = role("n5")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.remote.log-remote-lifecycle-events = ERROR
    akka.testconductor.barrier-timeout = 60 s
    """))

  testTransport(on = true)

}

class JepsenInspiredInsertSpecMultiJvmNode1 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode2 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode3 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode4 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode5 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode6 extends JepsenInspiredInsertSpec

class JepsenInspiredInsertSpec extends MultiNodeSpec(JepsenInspiredInsertSpec) with STMultiNodeSpec with ImplicitSender {
  import JepsenInspiredInsertSpec._
  import Replicator._

  override def initialParticipants = roles.size

  implicit val cluster = Cluster(system)
  val replicator = DataReplication(system).replicator
  val nodes = roles.drop(1) // controller not part of active nodes
  val nodeCount = nodes.size
  val timeout = 3.seconds.dilated
  val delayMillis = 0
  val totalCount = 200
  //  val delayMillis = 20
  //  val totalCount = 2000
  val expectedData = (0 until totalCount).toSet
  val data: Map[RoleName, Seq[Int]] = {
    val nodeIndex = (nodes.zipWithIndex map { case (n, i) => i -> n }).toMap
    (0 until totalCount).groupBy(i => nodeIndex(i % nodeCount))
  }
  lazy val myData: Seq[Int] = data(myself)

  def sleepDelay(): Unit =
    if (delayMillis != 0) {
      val rndDelay = ThreadLocalRandom.current().nextInt(delayMillis)
      if (rndDelay != 0) Thread.sleep(delayMillis)
    }

  def sleepBeforePartition(): Unit = {
    if (delayMillis != 0)
      Thread.sleep(delayMillis * totalCount / nodeCount / 10)
  }

  def sleepDuringPartition(): Unit =
    Thread.sleep(math.max(5000, delayMillis * totalCount / nodeCount / 2))

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "Insert from 5 nodes" must {

    "setup cluster" in {
      runOn(nodes: _*) {
        nodes foreach { join(_, n1) }

        within(10.seconds) {
          awaitAssert {
            replicator ! Internal.GetNodeCount
            expectMsg(Internal.NodeCount(nodes.size))
          }
        }
      }

      runOn(controller) {
        nodes foreach { n => enterBarrier(n.name + "-joined") }
      }

      enterBarrier("after-setup")
    }
  }

  "replicate values when all nodes connected" in {
    val key = "A"
    runOn(nodes: _*) {
      val writeProbe = TestProbe()
      val writeAcks = myData map { i =>
        sleepDelay()
        replicator.tell(Update(key, ORSet(), WriteOne, timeout, Some(i))(_ + i), writeProbe.ref)
        writeProbe.receiveOne(3.seconds)
      }
      val successWriteAcks = writeAcks collect { case success: UpdateSuccess => success }
      val failureWriteAcks = writeAcks collect { case fail: UpdateFailure => fail }
      successWriteAcks.map(_.request.get).toSet should be(myData.toSet)
      successWriteAcks.size should be(myData.size)
      failureWriteAcks should be(Nil)
      (successWriteAcks.size + failureWriteAcks.size) should be(myData.size)

      // eventually all nodes will have the data
      within(15.seconds) {
        awaitAssert {
          val readProbe = TestProbe()
          replicator.tell(Get(key, ReadOne, timeout), readProbe.ref)
          val result = readProbe.expectMsgPF() { case GetSuccess(key, set: ORSet, _) ⇒ set }
          result.value should be(expectedData)
        }
      }

    }

    enterBarrier("after-test-1")
  }

  "write/read to quorum when all nodes connected" in {
    val key = "B"
    runOn(nodes: _*) {
      val writeProbe = TestProbe()
      val writeAcks = myData map { i =>
        sleepDelay()
        replicator.tell(Update(key, ORSet(), WriteQuorum, timeout, Some(i))(_ + i), writeProbe.ref)
        writeProbe.receiveOne(timeout + 1.second)
      }
      val successWriteAcks = writeAcks collect { case success: UpdateSuccess => success }
      val failureWriteAcks = writeAcks collect { case fail: UpdateFailure => fail }
      successWriteAcks.map(_.request.get).toSet should be(myData.toSet)
      successWriteAcks.size should be(myData.size)
      failureWriteAcks should be(Nil)
      (successWriteAcks.size + failureWriteAcks.size) should be(myData.size)

      enterBarrier("data-written-2")

      // read from quorum of nodes, which is enough to retrieve all data
      val readProbe = TestProbe()
      replicator.tell(Get(key, ReadQuorum, timeout), readProbe.ref)
      val result = readProbe.expectMsgPF() { case GetSuccess(key, set: ORSet, _) ⇒ set }
      val survivors = result.value.size
      result.value should be(expectedData)

    }

    runOn(controller) {
      enterBarrier("data-written-2")
    }

    enterBarrier("after-test-2")
  }

  "replicate values after partition" in {
    val key = "C"
    runOn(controller) {
      sleepBeforePartition()
      for (a <- List(n1, n4, n5); b <- List(n2, n3))
        testConductor.blackhole(a, b, Direction.Both).await
      sleepDuringPartition()
      for (a <- List(n1, n4, n5); b <- List(n2, n3))
        testConductor.passThrough(a, b, Direction.Both).await
      enterBarrier("partition-healed-3")
    }

    runOn(nodes: _*) {
      val writeProbe = TestProbe()
      val writeAcks = myData map { i =>
        sleepDelay()
        replicator.tell(Update(key, ORSet(), WriteOne, timeout, Some(i))(_ + i), writeProbe.ref)
        writeProbe.receiveOne(3.seconds)
      }
      val successWriteAcks = writeAcks collect { case success: UpdateSuccess => success }
      val failureWriteAcks = writeAcks collect { case fail: UpdateFailure => fail }
      successWriteAcks.map(_.request.get).toSet should be(myData.toSet)
      successWriteAcks.size should be(myData.size)
      failureWriteAcks should be(Nil)
      (successWriteAcks.size + failureWriteAcks.size) should be(myData.size)

      enterBarrier("partition-healed-3")

      // eventually all nodes will have the data
      within(15.seconds) {
        awaitAssert {
          val readProbe = TestProbe()
          replicator.tell(Get(key, ReadOne, timeout), readProbe.ref)
          val result = readProbe.expectMsgPF() { case GetSuccess(key, set: ORSet, _) ⇒ set }
          result.value should be(expectedData)
        }
      }

    }

    enterBarrier("after-test-3")
  }

  "write to quorum during 3+2 partition and read from quorum after partition" in {
    val key = "D"
    runOn(controller) {
      sleepBeforePartition()
      for (a <- List(n1, n4, n5); b <- List(n2, n3))
        testConductor.blackhole(a, b, Direction.Both).await
      sleepDuringPartition()
      for (a <- List(n1, n4, n5); b <- List(n2, n3))
        testConductor.passThrough(a, b, Direction.Both).await
      enterBarrier("partition-healed-4")
    }

    runOn(nodes: _*) {
      val writeProbe = TestProbe()
      val writeAcks = myData map { i =>
        sleepDelay()
        replicator.tell(Update(key, ORSet(), WriteQuorum, timeout, Some(i))(_ + i), writeProbe.ref)
        writeProbe.receiveOne(timeout + 1.second)
      }
      val successWriteAcks = writeAcks collect { case success: UpdateSuccess => success }
      val failureWriteAcks = writeAcks collect { case fail: UpdateFailure => fail }
      runOn(n1, n4, n5) {
        successWriteAcks.map(_.request.get).toSet should be(myData.toSet)
        successWriteAcks.size should be(myData.size)
        failureWriteAcks should be(Nil)
      }
      runOn(n2, n3) {
        // without delays all could teoretically have been written before the blackhole
        if (delayMillis != 0)
          failureWriteAcks should not be (Nil)
      }
      (successWriteAcks.size + failureWriteAcks.size) should be(myData.size)

      enterBarrier("partition-healed-4")

      // on the 2 node side, read from quorum of nodes is enough to read all writes  
      runOn(n2, n3) {
        val readProbe = TestProbe()
        replicator.tell(Get(key, ReadQuorum, timeout), readProbe.ref)
        val result = readProbe.expectMsgPF() { case GetSuccess(key, set: ORSet, _) ⇒ set }
        val survivors = result.value.size
        result.value should be(expectedData)
      }
      // but on the 3 node side, read from quorum doesn't mean that we are guaranteed to see
      // the writes from the other side, yet

      // eventually all nodes will have the data
      within(15.seconds) {
        awaitAssert {
          val readProbe = TestProbe()
          replicator.tell(Get(key, ReadOne, timeout), readProbe.ref)
          val result = readProbe.expectMsgPF() { case GetSuccess(key, set: ORSet, _) ⇒ set }
          result.value should be(expectedData)
        }
      }
    }

    enterBarrier("after-test-4")
  }

}

