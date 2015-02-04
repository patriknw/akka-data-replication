/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.Cluster
import akka.cluster.UniqueAddress
import akka.util.HashCode

object PNCounter {
  val empty: PNCounter = new PNCounter(GCounter.empty, GCounter.empty)
  def apply(): PNCounter = empty
  /**
   * Java API
   */
  def create(): PNCounter = empty

  /**
   * Extract the [[GCounter#value]].
   */
  def unapply(c: PNCounter): Option[Long] = Some(c.value)
}

/**
 * Implements a 'Increment/Decrement Counter' CRDT, also called a 'PN-Counter'.
 *
 * PN-Counters allow the counter to be incremented by tracking the
 * increments (P) separate from the decrements (N). Both P and N are represented
 * as two internal [[GCounter]]s. Merge is handled by merging the internal P and N
 * counters. The value of the counter is the value of the P counter minus
 * the value of the N counter.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final class PNCounter private[akka] (
  private[akka] val increments: GCounter, private[akka] val decrements: GCounter)
  extends ReplicatedData with ReplicatedDataSerialization with RemovedNodePruning {

  type T = PNCounter

  /**
   * Current total value of the counter.
   */
  def value: Long = increments.value - decrements.value

  /**
   * Increment the counter with the delta specified.
   * If the delta is negative then it will decrement instead of increment.
   */
  def +(delta: Long)(implicit node: Cluster): PNCounter = increment(node, delta)

  /**
   * Increment the counter with the delta specified.
   * If the delta is negative then it will decrement instead of increment.
   */
  def increment(node: Cluster, delta: Long = 1): PNCounter =
    increment(node.selfUniqueAddress, delta)

  /**
   * Decrement the counter with the delta specified.
   * If the delta is negative then it will increment instead of decrement.
   */
  def -(delta: Long)(implicit node: Cluster): PNCounter = decrement(node, delta)

  /**
   * Decrement the counter with the delta specified.
   * If the delta is negative then it will increment instead of decrement.
   */
  def decrement(node: Cluster, delta: Long = 1): PNCounter =
    decrement(node.selfUniqueAddress, delta)

  private[akka] def increment(key: UniqueAddress, delta: Long): PNCounter = change(key, delta)
  private[akka] def increment(key: UniqueAddress): PNCounter = increment(key, 1)
  private[akka] def decrement(key: UniqueAddress, delta: Long): PNCounter = change(key, -delta)
  private[akka] def decrement(key: UniqueAddress): PNCounter = decrement(key, 1)

  private[akka] def change(key: UniqueAddress, delta: Long): PNCounter =
    if (delta > 0) copy(increments = increments.increment(key, delta))
    else if (delta < 0) copy(decrements = decrements.increment(key, -delta))
    else this

  override def merge(that: PNCounter): PNCounter =
    copy(increments = that.increments.merge(this.increments),
      decrements = that.decrements.merge(this.decrements))

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    increments.needPruningFrom(removedNode) || decrements.needPruningFrom(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): PNCounter =
    copy(increments = increments.prune(removedNode, collapseInto),
      decrements = decrements.prune(removedNode, collapseInto))

  override def pruningCleanup(removedNode: UniqueAddress): PNCounter =
    copy(increments = increments.pruningCleanup(removedNode),
      decrements = decrements.pruningCleanup(removedNode))

  private def copy(increments: GCounter = this.increments, decrements: GCounter = this.decrements): PNCounter =
    new PNCounter(increments, decrements)

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"PNCounter($value)"

  override def equals(o: Any): Boolean = o match {
    case other: PNCounter =>
      increments == other.increments && decrements == other.decrements
    case _ => false
  }

  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, increments)
    result = HashCode.hash(result, decrements)
    result
  }

}

