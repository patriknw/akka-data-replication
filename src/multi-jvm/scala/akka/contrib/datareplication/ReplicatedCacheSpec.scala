/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._
import com.typesafe.config.ConfigFactory

object ReplicatedCacheSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.log-dead-letters-during-shutdown = off
    """))

}

object ReplicatedCache {
  import akka.contrib.datareplication.Replicator._

  def props: Props = Props[ReplicatedCache]

  private final case class Request(key: String, replyTo: ActorRef)

  final case class PutInCache(key: String, value: Any)
  final case class GetFromCache(key: String)
  final case class Cached(key: String, value: Option[Any])
  final case class Evict(key: String)
}

class ReplicatedCache() extends Actor {
  import akka.contrib.datareplication.Replicator._
  import ReplicatedCache._

  val replicator = DataReplication(context.system).replicator
  implicit val cluster = Cluster(context.system)

  def dataKey(entryKey: String): String =
    "cache-" + math.abs(entryKey.hashCode) % 100

  def receive = {
    case PutInCache(key, value) =>
      replicator ! Update(dataKey(key), LWWMap.empty, WriteLocal)(_ + (key -> value))
    case Evict(key) =>
      replicator ! Update(dataKey(key), LWWMap.empty, WriteLocal)(_ - key)
    case GetFromCache(key) =>
      replicator ! Get(dataKey(key), ReadLocal, Some(Request(key, sender())))
    case GetSuccess(_, data: LWWMap, Some(Request(key, replyTo))) =>
      data.get(key) match {
        case Some(value) => replyTo ! Cached(key, Some(value))
        case None        => replyTo ! Cached(key, None)
      }
    case NotFound(_, Some(Request(key, replyTo))) =>
      replyTo ! Cached(key, None)
    case _: UpdateResponse => // ok
  }

}

class ReplicatedCacheSpecMultiJvmNode1 extends ReplicatedCacheSpec
class ReplicatedCacheSpecMultiJvmNode2 extends ReplicatedCacheSpec
class ReplicatedCacheSpecMultiJvmNode3 extends ReplicatedCacheSpec

class ReplicatedCacheSpec extends MultiNodeSpec(ReplicatedCacheSpec) with STMultiNodeSpec with ImplicitSender {
  import ReplicatedCacheSpec._
  import ReplicatedCache._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  val replicatedCache = system.actorOf(ReplicatedCache.props)

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "Demo of a replicated cache" must {
    "join cluster" in {
      join(node1, node1)
      join(node2, node1)
      join(node3, node1)

      enterBarrier("after-1")
    }

    "replicate cached entry" in within(10.seconds) {
      runOn(node1) {
        replicatedCache ! PutInCache("key1", "A")
      }

      awaitAssert {
        val probe = TestProbe()
        replicatedCache.tell(GetFromCache("key1"), probe.ref)
        probe.expectMsg(Cached("key1", Some("A")))
      }

      enterBarrier("after-2")
    }

    "replicate many cached entries" in within(10.seconds) {
      runOn(node1) {
        for (i <- 100 to 200)
          replicatedCache ! PutInCache("key" + i, i)
      }

      awaitAssert {
        val probe = TestProbe()
        for (i <- 100 to 200) {
          replicatedCache.tell(GetFromCache("key" + i), probe.ref)
          probe.expectMsg(Cached("key" + i, Some(i)))
        }
      }

      enterBarrier("after-3")
    }

    "replicate evicted entry" in within(15.seconds) {
      runOn(node1) {
        replicatedCache ! PutInCache("key2", "B")
      }

      awaitAssert {
        val probe = TestProbe()
        replicatedCache.tell(GetFromCache("key2"), probe.ref)
        probe.expectMsg(Cached("key2", Some("B")))
      }
      enterBarrier("key2-replicated")

      runOn(node3) {
        replicatedCache ! Evict("key2")
      }

      awaitAssert {
        val probe = TestProbe()
        replicatedCache.tell(GetFromCache("key2"), probe.ref)
        probe.expectMsg(Cached("key2", None))
      }

      enterBarrier("after-4")
    }

    "replicate updated cached entry" in within(10.seconds) {
      runOn(node2) {
        replicatedCache ! PutInCache("key1", "A2")
        replicatedCache ! PutInCache("key1", "A3")
      }

      awaitAssert {
        val probe = TestProbe()
        replicatedCache.tell(GetFromCache("key1"), probe.ref)
        probe.expectMsg(Cached("key1", Some("A3")))
      }

      enterBarrier("after-5")
    }

  }

}

