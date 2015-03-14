/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.Cluster
import akka.cluster.UniqueAddress
import scala.annotation.tailrec
import scala.collection.immutable.TreeMap
import akka.util.HashCode

// TODO this class can be optimized, but I wanted to start with correct functionality and comparability with riak_dt_orswot

object ORSet {
  private val _empty: ORSet[Any] = new ORSet(Map.empty, VectorClock.empty)
  def empty[A]: ORSet[A] = _empty.asInstanceOf[ORSet[A]]
  def apply(): ORSet[Any] = _empty
  /**
   * Java API
   */
  def create[A](): ORSet[A] = empty[A]

  /**
   * Extract the [[ORSet#elements]].
   */
  def unapply[A](s: ORSet[A]): Option[Set[A]] = Some(s.elements)

  /**
   * Extract the [[ORSet#elements]] of an `ORSet`.
   */
  def unapply(a: ReplicatedData): Option[Set[Any]] = a match {
    case s: ORSet[Any] @unchecked => Some(s.elements)
    case _                        => None
  }

  /**
   * INTERNAL API
   */
  private[akka]type Dot = VectorClock

  /**
   * INTERNAL API
   * Subtract the `vclock` from the `dot`.
   * What this means is that any (node, version) pair in
   * `dot` that is <= an entry in `vclock` is removed from `dot`.
   * Example [{a, 3}, {b, 2}, {d, 14}, {g, 22}] -
   *         [{a, 4}, {b, 1}, {c, 1}, {d, 14}, {e, 5}, {f, 2}] =
   *         [{b, 2}, {g, 22}]
   */
  private[akka] def subtractDots(dot: Dot, vclock: VectorClock): Dot = {

    @tailrec def dropDots(remaining: List[(UniqueAddress, Long)], acc: List[(UniqueAddress, Long)]): List[(UniqueAddress, Long)] =
      remaining match {
        case Nil ⇒ acc
        case (d @ (node, v1)) :: rest ⇒
          vclock.versions.get(node) match {
            case Some(v2) if v2 >= v1 ⇒
              // dot is dominated by clock, drop it
              dropDots(rest, acc)
            case _ ⇒
              dropDots(rest, d :: acc)
          }
      }

    val newDots = dropDots(dot.versions.toList, Nil)
    new VectorClock(versions = TreeMap.empty[UniqueAddress, Long] ++ newDots)
  }

  /**
   * INTERNAL API
   * @see [[ORSet#merge]]
   */
  private[akka] def mergeCommonKeys[A](commonKeys: Set[A], lhs: ORSet[A], rhs: ORSet[A]): Map[A, ORSet.Dot] = {
    commonKeys.foldLeft(Map.empty[A, ORSet.Dot]) {
      case (acc, k) ⇒
        val lhsDots = lhs.elementsMap(k).versions
        val rhsDots = rhs.elementsMap(k).versions
        val commonDots = lhsDots.filter {
          case (thisDotNode, v) ⇒ rhsDots.get(thisDotNode).exists(_ == v)
        }
        val commonDotsKeys = commonDots.keys
        val lhsUniqueDots = lhsDots -- commonDotsKeys
        val rhsUniqueDots = rhsDots -- commonDotsKeys
        val lhsKeep = ORSet.subtractDots(new VectorClock(lhsUniqueDots), rhs.vclock)
        val rhsKeep = ORSet.subtractDots(new VectorClock(rhsUniqueDots), lhs.vclock)
        val merged = lhsKeep.merge(rhsKeep).merge(new VectorClock(versions = commonDots))
        // Perfectly possible that an item in both sets should be dropped
        if (merged.versions.isEmpty) acc
        else acc.updated(k, merged)
    }
  }

  /**
   * INTERNAL API
   * @see [[ORSet#merge]]
   */
  private[akka] def mergeDisjointKeys[A](keys: Set[A], elementsMap: Map[A, ORSet.Dot], vclock: VectorClock,
                                         accumulator: Map[A, ORSet.Dot]): Map[A, ORSet.Dot] = {
    keys.foldLeft(accumulator) {
      case (acc, k) ⇒
        val dots = elementsMap(k)
        if (vclock > dots || vclock == dots)
          acc
        else {
          // Optimise the set of stored dots to include only those unseen
          val newDots = subtractDots(dots, vclock)
          acc.updated(k, newDots)
        }
    }
  }
}

/**
 * Implements a 'Observed Remove Set' CRDT, also called a 'OR-Set'.
 * Elements can be added and removed any number of times. Concurrent add wins
 * over remove.
 *
 * It is not implemented as in the paper. This is more space efficient
 * and don't accumulate garbage for removed elements. It is inspired by the
 * <a href="https://github.com/basho/riak_dt/blob/develop/src/riak_dt_orswot.erl">
 * riak_dt_orswot</a>.
 *
 * The ORSet has a version vector that is incremented when an element is added to
 * the set. The `node -> count` pair for that increment is stored against the
 * element as its "birth dot". Every time the element is re-added to the set,
 * its "birth dot" is updated to that of the `node -> count` version vector entry
 * resulting from the add. When an element is removed, we simply drop it, no tombstones.
 *
 * When an element exists in replica A and not replica B, is it because A added
 * it and B has not yet seen that, or that B removed it and A has not yet seen that?
 * In this implementation we compare the `dot` of the present element to the clock
 * in the Set it is absent from. If the element dot is not "seen" by the Set clock,
 * that means the other set has yet to see this add, and the item is in the merged
 * Set. If the Set clock dominates the dot, that means the other Set has removed this
 * element already, and the item is not in the merged Set.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final class ORSet[A] private[akka] (
  private[akka] val elementsMap: Map[A, ORSet.Dot],
  private[akka] val vclock: VectorClock)
  extends ReplicatedData with ReplicatedDataSerialization with RemovedNodePruning {

  type T = ORSet[A]

  /**
   * Scala API
   */
  def elements: Set[A] = elementsMap.keySet

  /**
   * Java API
   */
  def getElements(): java.util.Set[A] = {
    import scala.collection.JavaConverters._
    elements.asJava
  }

  def contains(a: A): Boolean = elementsMap.contains(a)

  def isEmpty: Boolean = elementsMap.isEmpty

  /**
   * Adds an element to the set
   */
  def +(element: A)(implicit node: Cluster): ORSet[A] = add(node, element)

  /**
   * Adds an element to the set
   */
  def add(node: Cluster, element: A): ORSet[A] = add(node.selfUniqueAddress, element)

  /**
   * INTERNAL API
   */
  private[akka] def add(node: UniqueAddress, element: A): ORSet[A] = {
    val newVclock = vclock + node
    val newDot = new VectorClock(versions = TreeMap(node -> newVclock.versions(node)))
    new ORSet(elementsMap = elementsMap.updated(element, newDot), vclock = newVclock)
  }

  /**
   * Removes an element from the set.
   */
  def -(element: A)(implicit node: Cluster): ORSet[A] = remove(node, element)

  /**
   * Removes an element from the set.
   */
  def remove(node: Cluster, element: A): ORSet[A] = remove(node.selfUniqueAddress, element)

  /**
   * INTERNAL API
   */
  private[akka] def remove(node: UniqueAddress, element: A): ORSet[A] =
    copy(elementsMap = elementsMap - element)

  /**
   * Removes all elements from the set, but keeps the history.
   * This has the same result as using [[#remove]] for each
   * element, but it is more efficient.
   */
  def clear(node: Cluster): ORSet[A] = clear(node.selfUniqueAddress)

  /**
   * INTERNAL API
   */
  private[akka] def clear(node: UniqueAddress): ORSet[A] = copy(elementsMap = Map.empty)

  /**
   * When element is in this Set but not in that Set:
   * Compare the "birth dot" of the present element to the clock in the Set it is absent from.
   * If the element dot is not "seen" by other Set clock, that means the other set has yet to
   * see this add, and the element is to be in the merged Set.
   * If the other Set clock dominates the dot, that means the other Set has removed
   * the element already, and the element is not to be in the merged Set.
   *
   * When element in both this Set and in that Set:
   * Some dots may still need to be shed. If this Set has dots that the other Set does not have,
   * and the other Set clock dominates those dots, then we need to drop those dots.
   * Keep only common dots, and dots that are not dominated by the other sides clock
   */
  override def merge(that: ORSet[A]): ORSet[A] = {
    val thisKeys = elementsMap.keySet
    val thatKeys = that.elementsMap.keySet
    val commonKeys = thisKeys.intersect(thatKeys)
    val thisUniqueKeys = thisKeys -- commonKeys
    val thatUniqueKeys = thatKeys -- commonKeys

    val entries00 = ORSet.mergeCommonKeys(commonKeys, this, that)
    val entries0 = ORSet.mergeDisjointKeys(thisUniqueKeys, this.elementsMap, that.vclock, entries00)
    val entries = ORSet.mergeDisjointKeys(thatUniqueKeys, that.elementsMap, this.vclock, entries0)
    val mergedVclock = this.vclock.merge(that.vclock)

    new ORSet(entries, mergedVclock)
  }

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    vclock.needPruningFrom(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): ORSet[A] = {
    val pruned = elementsMap.foldLeft(Map.empty[A, ORSet.Dot]) {
      case (acc, (elem, dot)) ⇒
        if (dot.needPruningFrom(removedNode)) acc.updated(elem, dot.prune(removedNode, collapseInto))
        else acc
    }
    if (pruned.isEmpty)
      copy(vclock = vclock.prune(removedNode, collapseInto))
    else {
      // re-add elements that were pruned, to bump dots to right vclock
      val newSet = new ORSet(elementsMap = elementsMap ++ pruned, vclock = vclock.prune(removedNode, collapseInto))
      pruned.keys.foldLeft(newSet) {
        case (s, elem) ⇒ s.add(collapseInto, elem)
      }
    }
  }

  override def pruningCleanup(removedNode: UniqueAddress): ORSet[A] = {
    val updated = elementsMap.foldLeft(elementsMap) {
      case (acc, (elem, dot)) ⇒
        if (dot.needPruningFrom(removedNode)) acc.updated(elem, dot.pruningCleanup(removedNode))
        else acc
    }
    new ORSet(updated, vclock.pruningCleanup(removedNode))
  }

  private def copy(elementsMap: Map[A, ORSet.Dot] = this.elementsMap, vclock: VectorClock = this.vclock): ORSet[A] =
    new ORSet(elementsMap, vclock)

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"OR$elements"

  override def equals(o: Any): Boolean = o match {
    case other: ORSet[_] => vclock == other.vclock && elementsMap == other.elementsMap
    case _               => false
  }

  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, elementsMap)
    result = HashCode.hash(result, vclock)
    result
  }
}

