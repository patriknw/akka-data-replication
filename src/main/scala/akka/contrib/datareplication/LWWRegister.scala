/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.Cluster
import akka.cluster.UniqueAddress

object LWWRegister {

  abstract class Clock {
    /**
     * @param currentTimestamp the current `timestamp` value of the `LWWRegister`
     */
    def nextTimestamp(currentTimestamp: Long): Long
  }

  final case class ClockValue(value: Long) extends Clock {
    override def nextTimestamp(currentTimestamp: Long): Long = value
  }

  /**
   * The default [[Clock]] is using max value of `System.currentTimeMillis()`
   * and `currentTimestamp + 1`.
   */
  val defaultClock = new Clock {
    override def nextTimestamp(currentTimestamp: Long): Long =
      math.max(System.currentTimeMillis(), currentTimestamp + 1)
  }

  /**
   * INTERNAL API
   */
  private[akka] def apply(node: UniqueAddress, initialValue: Any, clock: Clock): LWWRegister =
    new LWWRegister(node, initialValue, clock.nextTimestamp(0L))

  def apply(node: Cluster, initialValue: Any, clock: Clock = defaultClock): LWWRegister =
    apply(node.selfUniqueAddress, initialValue, clock)

  /**
   * Java API
   */
  def create(node: Cluster, initialValue: Any): LWWRegister =
    apply(node, initialValue)

  /**
   * Java API
   */
  def create(node: Cluster, initialValue: Any, clock: Clock): LWWRegister =
    apply(node, initialValue, clock)

  def unapply(value: Any): Option[Any] = value match {
    case r: LWWRegister ⇒ Some(r.value)
    case _              ⇒ None
  }

}

/**
 * Implements a 'Last Writer Wins Register' CRDT, also called a 'LWW-Register'.
 *
 * Merge takes the the register with highest timestamp. Note that this
 * relies on synchronized clocks. `LWWRegister` should only be used when the choice of
 * value is not important for concurrent updates occurring within the clock skew.
 *
 * Merge takes the register updated by the node with lowest address (`UniqueAddress` is ordered)
 * if the timestamps are exactly the same.
 *
 * Instead of using timestamps based on ´System.currentTimeMillis()` time it is possible to
 * use a timestamp value based on something else, for example an increasing version number
 * from a database record that is used for optimistic concurrency control.
 */
case class LWWRegister(
  private[akka] val node: UniqueAddress,
  private[akka] val state: Any,
  val timestamp: Long)
  extends ReplicatedData with ReplicatedDataSerialization {
  import LWWRegister.{ Clock, defaultClock }

  type T = LWWRegister

  /**
   * Scala API
   */
  def value: Any = state

  /**
   * Java API
   */
  def getValue(): AnyRef = state.asInstanceOf[AnyRef]

  def withValue(node: Cluster, value: Any): LWWRegister =
    withValue(node, value, defaultClock)

  def withValue(node: Cluster, value: Any, clock: Clock): LWWRegister =
    withValue(node.selfUniqueAddress, value, clock)

  def updatedBy: UniqueAddress = node

  /**
   * INTERNAL API
   */
  private[akka] def withValue(node: UniqueAddress, value: Any, clock: Clock): LWWRegister =
    copy(node = node, state = value, timestamp = clock.nextTimestamp(timestamp))

  override def merge(that: LWWRegister): LWWRegister =
    if (that.timestamp > this.timestamp) that
    else if (that.timestamp < this.timestamp) this
    else if (that.node < this.node) that
    else this
}

