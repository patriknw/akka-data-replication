Akka Data Replication
=====================

This is an **EARLY PREVIEW** of a library for replication of data in an Akka cluster.
It is a replicated in-memory data store supporting low latency and high availability
requirements. The data must be so called **Conflict Free Replicated Data Types** (CRDTs), 
i.e. they provide a monotonic merge function and the state changes always converge.

For good introduction to CRDTs you should watch the 
[Eventually Consistent Data Structures](http://www.google.com/url?q=http%3A%2F%2Fvimeo.com%2F43903960&sa=D&sntz=1&usg=AFQjCNF0yKi4WGCi3bhhdtLvBc33uVia6w)
talk by Sean Cribbs.

CRDTs can't be used for all types of problems, but when they can they have very nice properties:

- low latency of both read and writes
- high availability (partition tolerance)
- scalable (no central coordinator)
- strong eventual consistency (eventual consistency without conflicts)

Built in data types:

- Counters: `GCounter`, `PNCounter`
- Registers: `LWWRegister`, `Flag`
- Sets: `GSet`, `ORSet`
- Maps: `ORMap`, `LWWMap`, `PNCounterMap`

You can use your own custom data types by implementing the `merge` function of the `ReplicatedData`
trait. Note that CRDTs typically compose nicely, i.e. you can use the provided data types to build richer
data structures.

The `Replicator` actor implements the infrastructure for replication of the data. It uses
direct replication and gossip based dissemination. The `Replicator` actor is started on each node
in the cluster, or group of nodes tagged with a specific role. It communicates with other 
`Replicator` instances with the same path (without address) that are running on other nodes. 
For convenience it is typically used with the `DataReplication` Akka extension.

A short example of how to use it:

``` scala
class DataBot extends Actor with ActorLogging {
  import DataBot._
  import Replicator._

  val replicator = DataReplication(context.system).replicator
  implicit val cluster = Cluster(context.system)

  import context.dispatcher
  val tickTask = context.system.scheduler.schedule(5.seconds, 5.seconds, self, Tick)

  replicator ! Subscribe("key", self)

  var current = ORSet()

  def receive = {
    case Tick =>
      val s = ThreadLocalRandom.current().nextInt(97, 123).toChar.toString
      if (ThreadLocalRandom.current().nextBoolean()) {
        // add
        val newData = current + s
        log.info("Adding: {}", s)
        replicator ! Update("key", newData)
      } else {
        // remove
        val newData = current - s
        log.info("Removing: {}", s)
        replicator ! Update("key", newData)
      }

    case Changed("key", data: ORSet) =>
      current = data
      log.info("Current elements: {}", data.value)
  }

  override def postStop(): Unit = tickTask.cancel()

}
```
    
The full source code for this sample is in 
[DataBot.scala](https://github.com/patriknw/akka-datareplication/blob/v0.2/src/test/scala/akka/contrib/datareplication/sample/DataBot.scala).   

More detailed documentation can be found in the
[ScalaDoc](http://dl.bintray.com/patriknw/maven/com/github/patriknw/akka-datareplication_2.10/0.2/#akka-datareplication_2.10-0.2-javadoc.jar)
of `Replicator` and linked classes.

Two other examples:

- [VotingService](https://github.com/patriknw/akka-datareplication/blob/v0.2/src/multi-jvm/scala/akka/contrib/datareplication/VotingContestSpec.scala#L30)
- [ShoppingCart](https://github.com/patriknw/akka-datareplication/blob/v0.2/src/multi-jvm/scala/akka/contrib/datareplication/ReplicatedShoppingCartSpec.scala#L31)

Dependency
----------

Latest version of `akka-datareplication` is `0.2`. This version depends on Akka 2.3.4 and is
cross-built against Scala 2.10.4 and 2.11.0.

Add the following lines to your `build.sbt` file:

    resolvers += "patriknw at bintray" at "http://dl.bintray.com/patriknw/maven"

    libraryDependencies += "com.github.patriknw" %% "akka-datareplication" % "0.2"

More Resources
--------------

* [Eventually Consistent Data Structures](http://www.google.com/url?q=http%3A%2F%2Fvimeo.com%2F43903960&sa=D&sntz=1&usg=AFQjCNF0yKi4WGCi3bhhdtLvBc33uVia6w)
  talk by Sean Cribbs
* [Strong Eventual Consistency and Conflict-free Replicated Data Types](http://www.google.com/url?q=http%3A%2F%2Fresearch.microsoft.com%2Fapps%2Fvideo%2Fdl.aspx%3Fid%3D153540&sa=D&sntz=1&usg=AFQjCNFiwLpLjF-AQXPUm1Nmoy8hNIfrSQ)
  talk by Mark Shapiro
* [A comprehensive study of Convergent and Commutative Replicated Data Types](http://www.google.com/url?q=http%3A%2F%2Fhal.upmc.fr%2Fdocs%2F00%2F55%2F55%2F88%2FPDF%2Ftechreport.pdf&sa=D&sntz=1&usg=AFQjCNEGvFJ9I5m7yKpcAs8hcMP9Y5vy6A)
  paper by Mark Shapiro et. al. 
