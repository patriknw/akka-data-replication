package akka.contrib.datareplication

object HWWMap {

  val empty = HWWMap(Map.empty[String, HWWRegister].withDefaultValue(HWWRegister.initial))

  def apply(): HWWMap = empty
}

case class HWWMap(private[akka] val state: Map[String, HWWRegister]) extends ReplicatedData {
  override type T = HWWMap

  override def merge(that: T): T = {
    val allKeys = this.state.keySet ++ that.state.keySet
    var acc = Map.empty[String, HWWRegister]

    for (k <- allKeys) {
      (this.state.get(k), that.state.get(k)) match {
        case (Some(a), Some(b)) => acc += k -> a.merge(b)
        case (None, Some(b)) => acc += k -> b
        case (Some(a), None) => acc += k -> a
      }
    }

    HWWMap(acc)
  }



  def get(key: String): Int = state.get(key).map(_.value).getOrElse(0)

  /**
   * Adds an entry to the map
   */
  def put(key: String, value: Int): HWWMap =
    HWWMap(state + (key -> state(key).set(value)))

}
