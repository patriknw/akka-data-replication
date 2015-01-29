/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.{ UniqueAddress, Cluster }

object ORMultiMap {

  /**
   * Provides an empty multimap.
   */
  val empty: ORMultiMap =
    new ORMultiMap(ORMap.empty)
  def apply(): ORMultiMap = empty

  /**
   * Java API
   */
  def create(): ORMultiMap = empty

  def unapply(value: Any): Option[Map[String, Set[Any]]] = value match {
    case r: ORMultiMap ⇒ Some(r.entries)
    case _             ⇒ None
  }
}

/**
 * An immutable multi-map implementation. This class wraps an
 * [[ORMap]] with an [[ORSet]] for the map's value.
 */
final case class ORMultiMap private[akka] (private[akka] val underlying: ORMap)
  extends ReplicatedData with ReplicatedDataSerialization with RemovedNodePruning {

  override type T = ORMultiMap

  override def merge(that: T): T =
    new ORMultiMap(underlying.merge(that.underlying))

  /**
   * @return The entries of a multimap where keys are strings and values are untyped sets.
   */
  def entries: Map[String, Set[Any]] =
    underlying.entries.map { case (k, v: ORSet) ⇒ k -> v.value }

  /**
   * Java API
   */
  def getEntries(): java.util.Map[String, Set[Any]] = {
    import scala.collection.JavaConverters._
    entries.asJava
  }

  /**
   * Get the set associated with the key if there is one.
   */
  def get(key: String): Option[Set[Any]] =
    underlying.get(key).map { case v: ORSet ⇒ v.value }

  /**
   * Get the set associated with the key if there is one, else return the given default.
   */
  def getOrElse(key: String, default: ⇒ Set[Any]): Set[Any] =
    get(key).getOrElse(default)

  /**
   * Convenience for put. Requires an implicit Cluster.
   */
  def +[T](entry: (String, Set[T]))(implicit node: Cluster): ORMultiMap = {
    val (key, value) = entry
    put(node, key, value)
  }

  /**
   * Associate an entire set with the key while retaining the history of the previous
   * replicated data set.
   */
  def put[T](node: Cluster, key: String, value: Set[T]): ORMultiMap =
    put(node.selfUniqueAddress, key, value)

  /**
   * INTERNAL API
   */
  private[akka] def put[T](node: UniqueAddress, key: String, value: Set[T]): ORMultiMap = {
    val newUnderlying = underlying.updated(node, key, ORSet.empty) { existing =>
      value.foldLeft(existing.clear(node)) { (s, element) => s.add(node, element) }
    }
    ORMultiMap(newUnderlying)
  }

  /**
   * Convenience for remove. Requires an implicit Cluster.
   */
  def -(key: String)(implicit node: Cluster): ORMultiMap =
    remove(node, key)

  /**
   * Remove an entire set associated with the key.
   */
  def remove(node: Cluster, key: String): ORMultiMap =
    remove(node.selfUniqueAddress, key)

  /**
   * INTERNAL API
   */
  private[akka] def remove(node: UniqueAddress, key: String): ORMultiMap =
    ORMultiMap(underlying.remove(node, key))

  /**
   * Add an element to a set associated with a key. If there is no existing set then one will be initialised.
   */
  def addBinding(key: String, element: Any)(implicit cluster: Cluster): ORMultiMap =
    addBinding(cluster.selfUniqueAddress, key, element)

  /**
   * INTERNAL API
   */
  private[akka] def addBinding(node: UniqueAddress, key: String, element: Any): ORMultiMap = {
    val newUnderlying = underlying.updated(node, key, ORSet.empty)(_.add(node, element))
    ORMultiMap(newUnderlying)
  }

  /**
   * Remove an element of a set associated with a key. If there are no more elements in the set then the
   * entire set will be removed.
   */
  def removeBinding(key: String, element: Any)(implicit cluster: Cluster): ORMultiMap =
    removeBinding(cluster.selfUniqueAddress, key, element)

  /**
   * INTERNAL API
   */
  private[akka] def removeBinding(node: UniqueAddress, key: String, element: Any): ORMultiMap = {
    val newUnderlying = {
      val u = underlying.updated(node, key, ORSet.empty)(_.remove(node, element))
      u.get(key) match {
        case Some(s: ORSet) if s.isEmpty => u.remove(node, key)
        case _                           => u
      }
    }
    ORMultiMap(newUnderlying)
  }

  /**
   * Replace an element of a set associated with a key with a new one if it is different. This is useful when an element is removed
   * and another one is added within the same Update. The order of addition and removal is important in order
   * to retain history for replicated data.
   */
  def replaceBinding(key: String, oldElement: Any, newElement: Any)(implicit cluster: Cluster): ORMultiMap =
    replaceBinding(cluster.selfUniqueAddress, key, oldElement, newElement)

  /**
   * INTERNAL API
   */
  private[akka] def replaceBinding(node: UniqueAddress, key: String, oldElement: Any, newElement: Any): ORMultiMap =
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
