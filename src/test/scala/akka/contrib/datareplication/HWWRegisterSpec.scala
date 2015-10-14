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
         var right = x & 0xFFFF
         var left = x >>> 16

         @inline def feistelRound(): Unit = {
           val oldleft = left
           left = right ^ (fnv32(oldleft + epoch) & 0xFFFF)
           right = oldleft
         }

         feistelRound()
         feistelRound()
         feistelRound()
         feistelRound()

         (left << 16) | right
       }

       for (x <- sampling) {
         val xx = HWWRegister.permute(x, epoch = 42)
         depermute(xx, epoch = 42) should be(x)
       }
     }
  }

}
