/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication.sample

import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.contrib.datareplication.DataReplication
import akka.contrib.datareplication.ORSet
import akka.contrib.datareplication.Replicator
import com.typesafe.config.ConfigFactory

object LotsOfDataBot {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      startup(Seq("2551", "2552", "0"))
    else
      startup(args)
  }

  def startup(ports: Seq[String]): Unit = {
    ports.foreach { port =>
      // Override the configuration of the port
      val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port).
        withFallback(ConfigFactory.load(
          ConfigFactory.parseString("""
            passive = off
            max-entries = 100000
            akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
            akka.remote {
              netty.tcp {
                hostname = "127.0.0.1"
                port = 0
              }
            }

            akka.cluster {
              seed-nodes = [
                "akka.tcp://ClusterSystem@127.0.0.1:2551",
                "akka.tcp://ClusterSystem@127.0.0.1:2552"]

              auto-down-unreachable-after = 10s
            }
            akka.contrib.data-replication.use-offheap-memory = off
            akka.remote.log-frame-size-exceeding = 10000b
            """)))

      // Create an Akka system
      val system = ActorSystem("ClusterSystem", config)
      // Create an actor that handles cluster domain events
      system.actorOf(Props[LotsOfDataBot], name = "dataBot")
    }
  }

  private case object Tick

}

class LotsOfDataBot extends Actor with ActorLogging {
  import LotsOfDataBot._
  import Replicator._

  val replicator = DataReplication(context.system).replicator
  implicit val cluster = Cluster(context.system)

  import context.dispatcher
  val isPassive = context.system.settings.config.getBoolean("passive")
  var tickTask =
    if (isPassive)
      context.system.scheduler.schedule(1.seconds, 1.seconds, self, Tick)
    else
      context.system.scheduler.schedule(20.millis, 20.millis, self, Tick)

  val startTime = System.nanoTime()
  var count = 1L
  val maxEntries = context.system.settings.config.getInt("max-entries")

  def receive = if (isPassive) passive else active

  def active: Receive = {
    case Tick =>
      val loop = if (count >= maxEntries) 1 else 100
      for (_ <- 1 to loop) {
        count += 1
        if (count % 10000 == 0)
          log.info("Reached {} entries", count)
        if (count == maxEntries) {
          log.info("Reached {} entries", count)
          tickTask.cancel()
          tickTask = context.system.scheduler.schedule(1.seconds, 1.seconds, self, Tick)
        }
        val key = (count % maxEntries).toString
        if (count <= 100)
          replicator ! Subscribe(key, self)
        val s = ThreadLocalRandom.current().nextInt(97, 123).toChar.toString
        if (count <= maxEntries || ThreadLocalRandom.current().nextBoolean()) {
          // add
          replicator ! Update(key, ORSet(), WriteLocal)(_ + s)
        } else {
          // remove
          replicator ! Update(key, ORSet(), WriteLocal)(_ - s)
        }
      }

    case _: UpdateResponse => // ignore

    case Changed(key, ORSet(elements)) =>
      log.info("Current elements: {} -> {}", key, elements)
  }

  def passive: Receive = {
    case Tick =>
      if (!tickTask.isCancelled)
        replicator ! GetKeys
    case GetKeysResult(keys) =>
      if (keys.size >= maxEntries) {
        tickTask.cancel()
        val duration = (System.nanoTime() - startTime).nanos.toMillis
        log.info("It took {} ms to replicate {} entries", duration, keys.size)
      }
    case Changed(key, ORSet(elements)) =>
      log.info("Current elements: {} -> {}", key, elements)
  }

  override def postStop(): Unit = tickTask.cancel()

}

