package sleet.generators.reservedinstance;

import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperReservedInstanceIdGeneratorProxy {

  public static ZooKeeper getZk(ZooKeeperReservedInstanceIdGenerator zkGen) {
    return zkGen.zk;
  }
}
