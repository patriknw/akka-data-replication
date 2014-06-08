/**
 *  Copyright (C) 2014 Typesafe <http://typesafe.com/>
 */
package akka.contrib.datareplication;

import akka.cluster.Cluster;
import akka.cluster.Member;

/**
 * FIXME In Akka 2.3.4 akka.cluster.UniqueAddress will be public and then this
 * class will not be needed.
 */
public class UniqueAddressAccess {
  public static UniqueAddress memberUniqueAddress(Member member) {
    return new UniqueAddress(member.uniqueAddress().address(), member
        .uniqueAddress().uid());
  }

  public static UniqueAddress selfUniqueAddress(Cluster cluster) {
    return new UniqueAddress(cluster.selfUniqueAddress().address(), cluster
        .selfUniqueAddress().uid());
  }

}
