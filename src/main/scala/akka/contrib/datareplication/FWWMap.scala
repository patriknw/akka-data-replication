/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.Cluster
import akka.cluster.UniqueAddress

object FWWMap {
  private val _empty: FWWMap[Any] = new FWWMap(ORMap.empty)
  def empty[A]: FWWMap[A] = _empty.asInstanceOf[FWWMap[A]]
  def apply(): FWWMap[Any] = _empty
  /**
   * Java API
   */
  def create[A](): FWWMap[A] = empty

  /**
   * Extract the [[FWWMap#entries]].
   */
  def unapply[A](m: FWWMap[A]): Option[Map[String, A]] = Some(m.entries)
}

/**
 * Specialized [[ORMap]] with [[FWWRegister]] values.
 *
 * `FWWRegister` relies on synchronized clocks and should only be used when the choice of
 * value is not important for concurrent updates occurring within the clock skew.
 *
 * Instead of using timestamps based on `System.currentTimeMillis()` time it is possible to
 * use a timestamp value based on something else, for example an increasing version number
 * from a database record that is used for optimistic concurrency control.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final class FWWMap[A] private[akka] (
  private[akka] val underlying: ORMap[FWWRegister[A]])
  extends ReplicatedData with ReplicatedDataSerialization with RemovedNodePruning {
  import FWWRegister.{ Clock, defaultClock }

  type T = FWWMap[A]

  def entries: Map[String, A] = underlying.entries.map { case (k, r) ⇒ k -> r.value }

  def get(key: String): Option[A] = underlying.get(key).map(_.value)

  /**
   * Adds an entry to the map
   */
  def +(entry: (String, A))(implicit node: Cluster): FWWMap[A] = {
    val (key, value) = entry
    put(node, key, value)
  }

  /**
   * Adds an entry to the map
   */
  def put(node: Cluster, key: String, value: A): FWWMap[A] =
    put(node, key, value, defaultClock)

  /**
   * Adds an entry to the map.
   *
   * You can provide your `clock` implementation instead of using timestamps based
   * on `System.currentTimeMillis()` time. The timestamp can for example be an
   * increasing version number from a database record that is used for optimistic
   * concurrency control.
   */
  def put(node: Cluster, key: String, value: A, clock: Clock): FWWMap[A] =
    put(node.selfUniqueAddress, key, value, clock)

  /**
   * INTERNAL API
   */
  private[akka] def put(node: UniqueAddress, key: String, value: A, clock: Clock): FWWMap[A] = {
    val newRegister = underlying.get(key) match {
      case Some(r) ⇒ r.withValue(node, value, clock)
      case None    ⇒ FWWRegister(node, value, clock)
    }
    new FWWMap(underlying.put(node, key, newRegister))
  }

  /**
   * Removes an entry from the map.
   * Note that if there is a conflicting update on another node the entry will
   * not be removed after merge.
   */
  def -(key: String)(implicit node: Cluster): FWWMap[A] = remove(node, key)

  /**
   * Removes an entry from the map.
   * Note that if there is a conflicting update on another node the entry will
   * not be removed after merge.
   */
  def remove(node: Cluster, key: String): FWWMap[A] =
    remove(node.selfUniqueAddress, key)

  /**
   * INTERNAL API
   */
  private[akka] def remove(node: UniqueAddress, key: String): FWWMap[A] =
    new FWWMap(underlying.remove(node, key))

  override def merge(that: FWWMap[A]): FWWMap[A] =
    new FWWMap(underlying.merge(that.underlying))

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    underlying.needPruningFrom(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): FWWMap[A] =
    new FWWMap(underlying.prune(removedNode, collapseInto))

  override def pruningCleanup(removedNode: UniqueAddress): FWWMap[A] =
    new FWWMap(underlying.pruningCleanup(removedNode))

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"FWW$entries" //e.g. FWWMap(a -> 1, b -> 2)

  override def equals(o: Any): Boolean = o match {
    case other: FWWMap[_] => underlying == other.underlying
    case _                => false
  }

  override def hashCode: Int = underlying.hashCode
}

