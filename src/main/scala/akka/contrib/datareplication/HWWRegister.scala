package akka.contrib.datareplication

object HWWRegister {
  val initial = HWWRegister(0, 0)

  def apply(): HWWRegister = initial


  // FNV-1a hash used only as a simple and fast one-way function with good diffusion. Not cryptographically secure.
  private[akka] def fnv32(x: Int): Int = {
    var hash = 0x811C9DC5 // 2166136261
    @inline def round(x: Int): Unit = {
      hash ^= x
      hash *= 16777619
    }

    round(x & 0xFF)
    round((x >>> 8) & 0xFF)
    round((x >>> 16) & 0xFF)
    round(x >>> 24)
    hash
  }

  // Two rounds Feistel block to create a "random" permutation. This is not cryptographically safe, the Feistel
  // structure is only used to have an invertible function (permutation) Long => Long keyed by the epoch.
  private[akka] def permute(x: Int, epoch: Int): Int = {
    var right = x & 0xFFFF
    var left = x >>> 16

    @inline def feistelRound(): Unit = {
      val oldright = right
      right = left ^ (fnv32(oldright + epoch) & 0xFFFF)
      left = oldright
    }

    feistelRound()
    feistelRound()
    feistelRound()
    feistelRound()

    (left << 16) | right
  }

  // A "randomly" changing comparison function that is different in every epoch, but consistent in a certain epoch
  // values that are not equal originally will not be reported as equal after the permutation
  private def largerThan(x: Int, y: Int, epoch: Int): Boolean = permute(x, epoch) > permute(y, epoch)
}

case class HWWRegister(private[akka] val epoch: Int, value: Int) extends ReplicatedData {
  import HWWRegister._
  type T = HWWRegister

  private def earlierThan(that: HWWRegister): Boolean = {
    if (that.epoch > this.epoch) true
    else if (that.epoch < this.epoch) false
    else largerThan(that.value, this.value, epoch)
  }

  def set(newValue: Int): HWWRegister = {
    if (newValue == value) this
    else if (largerThan(newValue, value, epoch)) this.copy(value = newValue)
    else this.copy(epoch = epoch + 1, value = newValue)
  }

  def get(): Int = value

  override def merge(that: T): T = if (this earlierThan that) that else this
}
