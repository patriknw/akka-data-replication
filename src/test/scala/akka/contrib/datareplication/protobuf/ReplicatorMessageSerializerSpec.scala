/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication.protobuf

import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.contrib.datareplication.GSet
import akka.contrib.datareplication.PruningState
import akka.contrib.datareplication.PruningState.PruningInitialized
import akka.contrib.datareplication.PruningState.PruningPerformed
import akka.contrib.datareplication.Replicator._
import akka.contrib.datareplication.Replicator.Internal._
import akka.testkit.TestKit
import akka.util.ByteString
import akka.cluster.UniqueAddress
import com.typesafe.config.ConfigFactory

class ReplicatorMessageSerializerSpec extends TestKit(ActorSystem("ReplicatorMessageSerializerSpec",
  ConfigFactory.parseString("akka.actor.provider=akka.cluster.ClusterActorRefProvider")))
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  val serializer = new ReplicatorMessageSerializer(system.asInstanceOf[ExtendedActorSystem])

  val address1 = UniqueAddress(Address("akka.tcp", system.name, "some.host.org", 4711), 1)
  val address2 = UniqueAddress(Address("akka.tcp", system.name, "other.host.org", 4711), 2)
  val address3 = UniqueAddress(Address("akka.tcp", system.name, "some.host.org", 4712), 3)

  override def afterAll {
    shutdown()
  }

  def checkSerialization(obj: AnyRef): Unit = {
    val blob = serializer.toBinary(obj)
    val ref = serializer.fromBinary(blob, obj.getClass)
    ref should be(obj)
  }

  "ReplicatorMessageSerializer" must {

    "serialize Replicator messages" in {
      val ref1 = system.actorOf(Props.empty, "ref1")
      val data1 = GSet() + "a"

      checkSerialization(Get("A", ReadLocal))
      checkSerialization(Get("A", ReadQuorum(2.seconds), Some("x")))
      checkSerialization(GetSuccess("A", data1, None))
      checkSerialization(GetSuccess("A", data1, Some("x")))
      checkSerialization(NotFound("A", Some("x")))
      checkSerialization(GetFailure("A", Some("x")))
      checkSerialization(Subscribe("A", ref1))
      checkSerialization(Unsubscribe("A", ref1))
      checkSerialization(Changed("A", data1))
      checkSerialization(DataEnvelope(data1))
      checkSerialization(DataEnvelope(data1, pruning = Map(
        address1 -> PruningState(address2, PruningPerformed),
        address3 -> PruningState(address2, PruningInitialized(Set(address1.address))))))
      checkSerialization(Write("A", DataEnvelope(data1)))
      checkSerialization(WriteAck)
      checkSerialization(Read("A"))
      checkSerialization(ReadResult(Some(DataEnvelope(data1))))
      checkSerialization(ReadResult(None))
      checkSerialization(Status(Map("A" -> ByteString.fromString("a"),
        "B" -> ByteString.fromString("b")), chunk = 3, totChunks = 10))
      checkSerialization(Gossip(Map("A" -> DataEnvelope(data1),
        "B" -> DataEnvelope(GSet() + "b" + "c")), sendBack = true))
    }

  }
}
