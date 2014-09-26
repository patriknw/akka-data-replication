package akka.contrib.datareplication

object HWWFlag {
  val initial = HWWFlag(0L)

  def apply(): HWWFlag = initial

  /**
   * Java API
   */
  def create(): HWWFlag = initial
}

case class HWWFlag(state: Long) extends ReplicatedData {
  type T = HWWFlag

  def value: Boolean = (state & 1) == 1

  def setValue(b: Boolean): HWWFlag =
    if (b ^ value) HWWFlag(state + 1)
    else this

  override def merge(that: T): T = if (that.state > this.state) that else this
}
