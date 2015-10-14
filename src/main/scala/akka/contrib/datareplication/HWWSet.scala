package akka.contrib.datareplication

object HWWSet {
  val empty = HWWSet(Map.empty[Any, HWWFlag].withDefaultValue(HWWFlag.initial))

  def apply(): HWWSet = empty
}

case class HWWSet(private[akka] val state: Map[Any, HWWFlag]) extends ReplicatedData {
   type T = HWWSet

  /**
   * Monotonic merge function.
   */
  override def merge(that: T): T = {
    val allKeys = this.state.keySet ++ that.state.keySet
    var acc = Map.empty[Any, HWWFlag]

    for (k <- allKeys) {
      (this.state.get(k), that.state.get(k)) match {
        case (Some(a), Some(b)) => acc += k -> a.merge(b)
        case (None, Some(b)) => acc += k -> b
        case (Some(a), None) => acc += k -> a
      }
    }

    HWWSet(acc)
  }

  def contains(x: Any): Boolean = state.get(x).exists(_.value)

  lazy val value: Set[Any] = state.filter(_._2.value).keySet

  /**
   * Adds an element to the set
   */
  def add(element: Any): HWWSet =
    HWWSet(state + (element -> state(element).setValue(true)))

  /**
   * Removes an element from the set
   */
  def remove(element: Any): HWWSet =
    HWWSet(state + (element -> state(element).setValue(false)))

}
