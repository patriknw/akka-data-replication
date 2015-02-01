/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.{ UniqueAddress, Cluster }

object ORMultiMap {

  val _empty: ORMultiMap[Any] = new ORMultiMap(ORMap.empty)
  /**
   * Provides an empty multimap.
   */
  def empty[A]: ORMultiMap[A] = _empty.asInstanceOf[ORMultiMap[A]]
  def apply(): ORMultiMap[Any] = _empty

  /**
   * Java API
   */
  def create[A](): ORMultiMap[A] = empty[A]

  def unapply(value: Any): Option[Map[String, Set[Any]]] = value match {
    case r: ORMultiMap[Any] @unchecked ⇒ Some(r.entries)
    case _                             ⇒ None
  }
}

/**
 * An immutable multi-map implementation. This class wraps an
 * [[ORMap]] with an [[ORSet]] for the map's value.
 */
final case class ORMultiMap[A] private[akka] (private[akka] val underlying: ORMap[ORSet[A]])
  extends ReplicatedData with ReplicatedDataSerialization with RemovedNodePruning {

  override type T = ORMultiMap[A]

  override def merge(that: T): T =
    new ORMultiMap(underlying.merge(that.underlying))

  /**
   * @return The entries of a multimap where keys are strings and values are untyped sets.
   */
  def entries: Map[String, Set[A]] =
    underlying.entries.map { case (k, v) ⇒ k -> v.value }

  /**
   * Java API
   */
  def getEntries(): java.util.Map[String, Set[A]] = {
    import scala.collection.JavaConverters._
    entries.asJava
  }

  /**
   * Get the set associated with the key if there is one.
   */
  def get(key: String): Option[Set[A]] =
    underlying.get(key).map(_.value)

  /**
   * Get the set associated with the key if there is one, else return the given default.
   */
  def getOrElse(key: String, default: ⇒ Set[A]): Set[A] =
    get(key).getOrElse(default)

  /**
   * Convenience for put. Requires an implicit Cluster.
   */
  def +(entry: (String, Set[A]))(implicit node: Cluster): ORMultiMap[A] = {
    val (key, value) = entry
    put(node, key, value)
  }

  /**
   * Associate an entire set with the key while retaining the history of the previous
   * replicated data set.
   */
  def put(node: Cluster, key: String, value: Set[A]): ORMultiMap[A] =
    put(node.selfUniqueAddress, key, value)

  /**
   * INTERNAL API
   */
  private[akka] def put(node: UniqueAddress, key: String, value: Set[A]): ORMultiMap[A] = {
    val newUnderlying = underlying.updated(node, key, ORSet.empty[A]) { existing =>
      value.foldLeft(existing.clear(node)) { (s, element) => s.add(node, element) }
    }
    ORMultiMap(newUnderlying)
  }

  /**
   * Convenience for remove. Requires an implicit Cluster.
   */
  def -(key: String)(implicit node: Cluster): ORMultiMap[A] =
    remove(node, key)

  /**
   * Remove an entire set associated with the key.
   */
  def remove(node: Cluster, key: String): ORMultiMap[A] =
    remove(node.selfUniqueAddress, key)

  /**
   * INTERNAL API
   */
  private[akka] def remove(node: UniqueAddress, key: String): ORMultiMap[A] =
    ORMultiMap(underlying.remove(node, key))

  /**
   * Add an element to a set associated with a key. If there is no existing set then one will be initialised.
   */
  def addBinding(key: String, element: A)(implicit cluster: Cluster): ORMultiMap[A] =
    addBinding(cluster.selfUniqueAddress, key, element)

  /**
   * INTERNAL API
   */
  private[akka] def addBinding(node: UniqueAddress, key: String, element: A): ORMultiMap[A] = {
    val newUnderlying = underlying.updated(node, key, ORSet.empty[A])(_.add(node, element))
    ORMultiMap(newUnderlying)
  }

  /**
   * Remove an element of a set associated with a key. If there are no more elements in the set then the
   * entire set will be removed.
   */
  def removeBinding(key: String, element: A)(implicit cluster: Cluster): ORMultiMap[A] =
    removeBinding(cluster.selfUniqueAddress, key, element)

  /**
   * INTERNAL API
   */
  private[akka] def removeBinding(node: UniqueAddress, key: String, element: A): ORMultiMap[A] = {
    val newUnderlying = {
      val u = underlying.updated(node, key, ORSet.empty[A])(_.remove(node, element))
      u.get(key) match {
        case Some(s) if s.isEmpty => u.remove(node, key)
        case _                    => u
      }
    }
    ORMultiMap(newUnderlying)
  }

  /**
   * Replace an element of a set associated with a key with a new one if it is different. This is useful when an element is removed
   * and another one is added within the same Update. The order of addition and removal is important in order
   * to retain history for replicated data.
   */
  def replaceBinding(key: String, oldElement: A, newElement: A)(implicit cluster: Cluster): ORMultiMap[A] =
    replaceBinding(cluster.selfUniqueAddress, key, oldElement, newElement)

  /**
   * INTERNAL API
   */
  private[akka] def replaceBinding(node: UniqueAddress, key: String, oldElement: A, newElement: A): ORMultiMap[A] =
    if (newElement != oldElement)
      addBinding(node, key, newElement).removeBinding(node, key, oldElement)
    else
      this

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    underlying.needPruningFrom(removedNode)

  override def pruningCleanup(removedNode: UniqueAddress): T =
    new ORMultiMap(underlying.pruningCleanup(removedNode))

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): T =
    new ORMultiMap(underlying.prune(removedNode, collapseInto))
}
