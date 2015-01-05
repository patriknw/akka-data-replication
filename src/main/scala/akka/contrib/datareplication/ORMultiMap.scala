/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.{UniqueAddress, Cluster}

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
case class ORMultiMap private (private[akka] val map: ORMap)
    extends ReplicatedData with RemovedNodePruning with Serializable {

  override type T = ORMultiMap

  override def merge(that: T): T =
    new ORMultiMap(map.merge(that.map))

  /**
   * @return The entries of a multimap where keys are strings and values are untyped sets.
   */
  def entries: Map[String, Set[Any]] =
    map.entries.map { case (k, v: ORSet) ⇒ k -> v.value }

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
    map.get(key).map { case v: ORSet ⇒ v.value }

  /**
   * Convenience for put. Requires an implicit Cluster.
   */
  def +(entry: (String, Set[Any]))(implicit node: Cluster): ORMultiMap = {
    val (key, value) = entry
    put(node, key, value)
  }

  /**
   * Associate an entire set with the key while retaining the history of the previous
   * replicated data set.
   */
  def put(node: Cluster, key: String, value: Set[Any]): ORMultiMap =
    put(node.selfUniqueAddress, key, value)

  /*
   * Remove all elements of a set and then merge in elements of a new set so that we may retain the replicated
   * data history throughout.
   * TODO: We may wish to consider optimising this as the code isn't the most efficient. Ideally we should be able to remove all entries of an ORSet in one go. The same goes for adding to an ORSet in bulk.
   */
  private def removeAndMerge(node: UniqueAddress, newValue: ORSet)(oldValue: ORSet): ORSet =
    oldValue.value.diff(newValue.value)
      .foldLeft(oldValue) { (value, element) ⇒ value.remove(node, element) }
      .merge(newValue)

  private[akka] def put(node: UniqueAddress, key: String, value: Set[Any]): ORMultiMap = {
    val newValue = value.foldLeft(ORSet.empty)((v, e) ⇒ v.add(node, e))
    val values = updateOrInit(key, removeAndMerge(node, newValue), newValue)
    ORMultiMap(map.put(node, key, values))
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

  private[akka] def remove(node: UniqueAddress, key: String): ORMultiMap =
    ORMultiMap(map.remove(node, key))

  /**
   * Add an element to a set associated with a key. If there is no existing set then one will be initialised.
   */
  def addBinding(key: String, element: Any)(implicit cluster: Cluster): ORMultiMap =
    addBinding(cluster.selfUniqueAddress, key, element)

  private[akka] def addBinding(node: UniqueAddress, key: String, element: Any): ORMultiMap = {
    val values = updateOrInit(key, _.add(node, element), ORSet.empty.add(node, element))
    ORMultiMap(map.put(node, key, values))
  }

  /**
   * Remove an element of a set associated with a key. If there are no more elements in the set then the
   * entire set will be removed.
   */
  def removeBinding(key: String, element: Any)(implicit cluster: Cluster): ORMultiMap =
    removeBinding(cluster.selfUniqueAddress, key, element)

  private[akka] def removeBinding(node: UniqueAddress, key: String, element: Any): ORMultiMap = {
    val values = updateOrInit(key, _.remove(node, element), ORSet.empty)
    if (values.value.nonEmpty)
      ORMultiMap(map.put(node, key, values))
    else
      ORMultiMap(map.remove(node, key))
  }

  /**
   * Replace an element of a set associated with a key with a new one if it is different. This is useful when an element is removed
   * and another one is added within the same Update. The order of addition and removal is important in order
   * to retain history for replicated data.
   */
  def replaceBinding(key: String, oldElement: Any, newElement: Any)(implicit cluster: Cluster): ORMultiMap =
    replaceBinding(cluster.selfUniqueAddress, key, oldElement, newElement)

  private[akka] def replaceBinding(node: UniqueAddress, key: String, oldElement: Any, newElement: Any): ORMultiMap =
    if (newElement != oldElement)
      addBinding(node, key, newElement).removeBinding(node, key, oldElement)
    else
      this

  private def updateOrInit(key: String, update: ORSet ⇒ ORSet, init: ⇒ ORSet): ORSet =
    map.get(key).asInstanceOf[Option[ORSet]] match {
      case Some(values) ⇒ update(values)
      case None         ⇒ init
    }

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    map.needPruningFrom(removedNode)

  override def pruningCleanup(removedNode: UniqueAddress): T =
    new ORMultiMap(map.pruningCleanup(removedNode))

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): T =
    new ORMultiMap(map.prune(removedNode, collapseInto))
}
