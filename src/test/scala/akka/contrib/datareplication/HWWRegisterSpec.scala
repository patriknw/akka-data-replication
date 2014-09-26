package akka.contrib.datareplication

import org.scalatest.{Matchers, WordSpec}

class HWWRegisterSpec extends WordSpec with Matchers {

  "HWWRegister unit test" must {
     import HWWRegister._

     "verify that FNV-1a is implemented properly" in {
       // Refereence values from http://www.isthe.com/chongo/tech/comp/fnv/
       fnv32(0xC43124CC) should be(0)
       fnv32(0xCB9F4DE0) should be(0)
     }

     "verify that permute is invertible" in {
       val sampling = 0 to Int.MaxValue by 65537

       def depermute(x: Int, epoch: Int): Int = {
         val right1 = x & 0xFFFF
         val left1 = x >>> 16

         // Feistel round 1
         val right2 = left1
         val left2 = right1 ^ (fnv32(left1 + epoch) & 0xFFFF)

         // Feistel round 2
         val right3 = left2
         val left3 = right2 ^ (fnv32(left2 + epoch) & 0xFFFF)

         (left3 << 16) | right3
       }

       for (x <- sampling) {
         val xx = HWWRegister.permute(x, epoch = 42)
         depermute(xx, epoch = 42) should be(x)
       }
     }
  }

}
