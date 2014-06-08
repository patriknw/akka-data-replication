/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

import akka.cluster.Member
import akka.actor.Address
import akka.cluster.Cluster

/**
 * FIXME In Akka 2.3.4 akka.cluster.UniqueAddress will be public
 * and then this class will be replaced by that.
 *
 * Member identifier consisting of address and random `uid`.
 * The `uid` is needed to be able to distinguish different
 * incarnations of a member with same hostname and port.
 */
@SerialVersionUID(1L)
final case class UniqueAddress(address: Address, uid: Int) extends Ordered[UniqueAddress] {
  override def hashCode = uid

  def compare(that: UniqueAddress): Int = {
    val result = Member.addressOrdering.compare(this.address, that.address)
    if (result == 0) if (this.uid < that.uid) -1 else if (this.uid == that.uid) 0 else 1
    else result
  }
}

