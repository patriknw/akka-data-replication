/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.datareplication

import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Stash
import akka.cluster.Cluster
import akka.testkit.ImplicitSender
import akka.testkit.TestKit

object LocalConcurrencySpec {

  case class Add(s: String)

  class Updater extends Actor with Stash {
    implicit val cluster = Cluster(context.system)
    val replicator = DataReplication(context.system).replicator
    val key = "key"

    def receive = {
      case s: String =>
        val update = Replicator.Update(key, ORSet.empty, Replicator.WriteLocal)(_ + s)
        replicator ! update
    }
  }
}

class LocalConcurrencySpec(_system: ActorSystem) extends TestKit(_system)
  with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {
  import LocalConcurrencySpec._

  def this() {
    this(ActorSystem("LocalConcurrencySpec",
      ConfigFactory.parseString("""
      akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
      """)))
  }

  override def afterAll(): Unit = {
    system.shutdown()
    system.awaitTermination(10.seconds)
  }

  val replicator = DataReplication(system).replicator

  "Updates from same node" must {

    "be possible to do from two actors" in {
      val updater1 = system.actorOf(Props[Updater], "updater1")
      val updater2 = system.actorOf(Props[Updater], "updater2")

      val numMessages = 100
      for (n <- 1 to numMessages) {
        updater1 ! s"a$n"
        updater2 ! s"b$n"
      }

      val expected = ((1 to numMessages).map("a" + _) ++ (1 to numMessages).map("b" + _)).toSet
      awaitAssert {
        replicator ! Replicator.Get("key", Replicator.ReadLocal)
        val ORSet(elements) = expectMsgType[Replicator.GetSuccess].data
        elements should be(expected)
      }

    }

  }
}
