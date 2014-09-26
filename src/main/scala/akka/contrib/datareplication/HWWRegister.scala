package akka.contrib.datareplication

object HWWRegister {
  val initial = HWWRegister(0, 0)

  def apply(): HWWRegister = initial


  // FNV-1a hash used only as a simple and fast one-way function with good diffusion. Not cryptographically secure.
  private[akka] def fnv32(x: Int): Int = {
    var hash = 0x811C9DC5 // 2166136261
    hash ^= (x & 0xFF)
    hash *= 16777619
    hash ^= (x >>> 8) & 0xFF
    hash *= 16777619
    hash ^= (x >>> 16) & 0xFF
    hash *= 16777619
    hash ^= x >>> 24
    hash *= 16777619
    hash
  }

  // Two rounds Feistel block to create a "random" permutation. This is not cryptographically safe, the Feistel
  // structure is only used to have an invertible function (permutation) Long => Long keyed by the epoch.
  private[akka] def permute(x: Int, epoch: Int): Int = {
    val right1 = x & 0xFFFF
    val left1 = x >>> 16

    // Feistel round 1
    val right2 = left1 ^ (fnv32(right1 + epoch) & 0xFFFF)
    val left2 = right1

    // Feistel round 2
    val right3 = left2 ^ (fnv32(right2 + epoch) & 0xFFFF)
    val left3 = right2

    (left3 << 16) | right3
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
