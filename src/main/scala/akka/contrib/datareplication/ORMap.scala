/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.Cluster
import akka.cluster.UniqueAddress

object ORMap {
  private val _empty: ORMap[ReplicatedData] = new ORMap(ORSet.empty, Map.empty)
  def empty[A <: ReplicatedData]: ORMap[A] = _empty.asInstanceOf[ORMap[A]]
  def apply(): ORMap[ReplicatedData] = _empty
  /**
   * Java API
   */
  def create[A <: ReplicatedData](): ORMap[A] = empty[A]

  def unapply(value: Any): Option[Map[String, ReplicatedData]] = value match {
    case r: ORMap[ReplicatedData] @unchecked ⇒ Some(r.entries)
    case _                                   ⇒ None
  }
}

/**
 * Implements a 'Observed Remove Map' CRDT, also called a 'OR-Map'.
 *
 * It has similar semantics as an [[ORSet]], but in case
 * concurrent updates the values are merged, and must therefore be [[ReplicatedData]]
 * themselves.
 */
final case class ORMap[A <: ReplicatedData](
  private[akka] val keys: ORSet[String],
  private[akka] val values: Map[String, A])
  extends ReplicatedData with ReplicatedDataSerialization with RemovedNodePruning {

  type T = ORMap[A]

  /**
   * Scala API
   */
  def entries: Map[String, A] = values

  /**
   * Java API
   */
  def getEntries(): java.util.Map[String, A] = {
    import scala.collection.JavaConverters._
    entries.asJava
  }

  def get(key: String): Option[A] = values.get(key)

  def getOrElse(key: String, default: => A): A = values.getOrElse(key, default)

  def contains(key: String): Boolean = values.contains(key)

  def isEmpty: Boolean = values.isEmpty

  /**
   * Adds an entry to the map
   */
  def +(entry: (String, A))(implicit node: Cluster): ORMap[A] = {
    val (key, value) = entry
    put(node, key, value)
  }

  /**
   * Adds an entry to the map.
   * Note that the new `value` will be merged with existing values
   * on other nodes and the outcome depends on what `ReplicatedData`
   * type that is used.
   *
   * Consider using [[#updated]] instead of `put` if you want modify
   * existing entry.
   *
   * `IllegalArgumentException` is thrown if you try to replace an existing `ORSet`
   * value, because important history can be lost when replacing the `ORSet` and
   * undesired effects of merging will occur. Use [[ORMultiMap]] or [[#updated]] instead.
   */
  def put(node: Cluster, key: String, value: A): ORMap[A] = put(node.selfUniqueAddress, key, value)

  /**
   * INTERNAL API
   */
  private[akka] def put(node: UniqueAddress, key: String, value: A): ORMap[A] =
    if (value.isInstanceOf[ORSet[_]] && values.contains(key))
      throw new IllegalArgumentException(
        "`ORMap.put` must not be used to replace an existing `ORSet` " +
          "value, because important history can be lost when replacing the `ORSet` and " +
          "undesired effects of merging will occur. Use `ORMultiMap` or `ORMap.updated` instead.")
    else
      ORMap(keys.add(node, key), values.updated(key, value))

  /**
   * Scala API: Replace a value by applying the `modify` function on the existing value.
   *
   * If there is no current value for the `key` the `initial` value will be
   * passed to the `modify` function.
   */
  def updated(node: Cluster, key: String, initial: A)(modify: A => A): ORMap[A] =
    updated(node.selfUniqueAddress, key, initial)(modify)

  /**
   * Java API: Replace a value by applying the `modify` function on the existing value.
   *
   * If there is no current value for the `key` the `initial` value will be
   * passed to the `modify` function.
   */
  def updated(node: Cluster, key: String, initial: A, modify: akka.japi.Function[A, A]): ORMap[A] =
    updated(node, key, initial)(value => modify.apply(value))

  /**
   * INTERNAL API
   */
  private[akka] def updated(node: UniqueAddress, key: String, initial: A)(modify: A => A): ORMap[A] = {
    val newValue = values.get(key) match {
      case Some(old) => modify(old)
      case _         => modify(initial)
    }
    ORMap(keys.add(node, key), values.updated(key, newValue))
  }

  /**
   * Removes an entry from the map.
   * Note that if there is a conflicting update on another node the entry will
   * not be removed after merge.
   */
  def -(key: String)(implicit node: Cluster): ORMap[A] = remove(node, key)

  /**
   * Removes an entry from the map.
   * Note that if there is a conflicting update on another node the entry will
   * not be removed after merge.
   */
  def remove(node: Cluster, key: String): ORMap[A] = remove(node.selfUniqueAddress, key)

  /**
   * INTERNAL API
   */
  private[akka] def remove(node: UniqueAddress, key: String): ORMap[A] = {
    ORMap(keys.remove(node, key), values - key)
  }

  override def merge(that: ORMap[A]): ORMap[A] = {
    val mergedKeys = keys.merge(that.keys)
    var mergedValues = Map.empty[String, A]
    mergedKeys.elements.keysIterator.foreach {
      case key: String ⇒
        (this.values.get(key), that.values.get(key)) match {
          case (Some(thisValue), Some(thatValue)) ⇒
            if (thisValue.getClass != thatValue.getClass) {
              val errMsg = s"Wrong type for merging [$key] in [${getClass.getName}], existing type " +
                s"[${thisValue.getClass.getName}], got [${thatValue.getClass.getName}]"
              throw new IllegalArgumentException(errMsg)
            }
            val mergedValue = thisValue.merge(thatValue.asInstanceOf[thisValue.T]).asInstanceOf[A]
            mergedValues = mergedValues.updated(key, mergedValue)
          case (Some(thisValue), None) ⇒
            mergedValues = mergedValues.updated(key, thisValue)
          case (None, Some(thatValue)) ⇒
            mergedValues = mergedValues.updated(key, thatValue)
          case (None, None) ⇒ throw new IllegalStateException(s"missing value for $key")
        }
    }

    ORMap(mergedKeys, mergedValues)
  }

  override def needPruningFrom(removedNode: UniqueAddress): Boolean = {
    keys.needPruningFrom(removedNode) || values.exists {
      case (_, data: RemovedNodePruning) ⇒ data.needPruningFrom(removedNode)
      case _                             ⇒ false
    }
  }

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): ORMap[A] = {
    val prunedKeys = keys.prune(removedNode, collapseInto)
    val prunedValues = values.foldLeft(values) {
      case (acc, (key, data: RemovedNodePruning)) if data.needPruningFrom(removedNode) ⇒
        acc.updated(key, data.prune(removedNode, collapseInto).asInstanceOf[A])
      case (acc, _) ⇒ acc
    }
    ORMap(prunedKeys, prunedValues)
  }

  override def pruningCleanup(removedNode: UniqueAddress): ORMap[A] = {
    val pruningCleanupedKeys = keys.pruningCleanup(removedNode)
    val pruningCleanupedValues = values.foldLeft(values) {
      case (acc, (key, data: RemovedNodePruning)) if data.needPruningFrom(removedNode) ⇒
        acc.updated(key, data.pruningCleanup(removedNode).asInstanceOf[A])
      case (acc, _) ⇒ acc
    }
    ORMap(pruningCleanupedKeys, pruningCleanupedValues)
  }
}

