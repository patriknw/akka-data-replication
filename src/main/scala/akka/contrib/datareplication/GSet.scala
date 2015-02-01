/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

object GSet {
  private val _empty: GSet[Any] = new GSet(Set.empty)
  def empty[A]: GSet[A] = _empty.asInstanceOf[GSet[A]]
  def apply(): GSet[Any] = _empty
  /**
   * Java API
   */
  def create[A](): GSet[A] = empty[A]

  def unapply(value: Any): Option[Set[Any]] = value match {
    case s: GSet[Any] @unchecked ⇒ Some(s.value)
    case _                       ⇒ None
  }
}

/**
 * Implements a 'Add Set' CRDT, also called a 'G-Set'. You can't
 * remove an element of a G-Set.
 *
 * A G-Set doesn't accumulate any garbage apart from the elements themselves.
 */
final case class GSet[A](elements: Set[A]) extends ReplicatedData with ReplicatedDataSerialization {

  type T = GSet[A]

  /**
   * Scala API
   */
  def value: Set[A] = elements

  /**
   * Java API
   */
  def getValue(): java.util.Set[A] = {
    import scala.collection.JavaConverters._
    value.asJava
  }

  def contains(a: A): Boolean = elements(a)

  /**
   * Adds an element to the set
   */
  def +(element: A): GSet[A] = add(element)

  /**
   * Adds an element to the set
   */
  def add(element: A): GSet[A] = copy(elements + element)

  override def merge(that: GSet[A]): GSet[A] = copy(elements ++ that.elements)
}

