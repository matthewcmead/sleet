package sleet.utils.curator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;

import sleet.utils.curator.InstanceIdManager.IdContentionPolicy;
import sleet.utils.curator.InstanceIdManager.ManagedInstanceIdUser;

/**
 * A class to manage (over zookeeper) the act of allocating a currently unique
 * instance id within a single zookeeper quorum.
 * 
 * @author mmead
 * 
 */

public class TestInstanceIdManager {
  private static final long TIMEOUT = 1000L;
  private static final int NUM_IDS = 4;

  public static void main(String[] args) throws Exception {
    final long start = System.currentTimeMillis();
    ZkMiniCluster cluster = new ZkMiniCluster();
    File temp = File.createTempFile("instanceidmanager", "");
    temp.delete();
    temp.mkdir();
    System.out.println("zk temp path: " + temp.getAbsolutePath());
    cluster.startZooKeeper(temp.getAbsolutePath());
    String zkconn = cluster.getZkConnectionString();
    zkconn = "localhost:" + zkconn.substring(zkconn.lastIndexOf(':') + 1);
    System.out.println("ZK connection string: " + zkconn);
    final CuratorFramework client = CuratorFrameworkFactory.instance(zkconn, (int) TIMEOUT, 1);
    final List<InstanceIdManager> idms = new ArrayList<InstanceIdManager>();
    Thread t = new Thread() {

      @Override
      public void run() {
        for (int i = 0; i < NUM_IDS * 5; i++) {
          InstanceIdManager idm = new InstanceIdManager("/sleet/datacenter/1", client, new ManagedInstanceIdUser() {
            private int id;
            private Object idLock = new Object();

            @Override
            public void idInvalidated(InstanceIdManager idm) {
              idm.report(idm, "Thread: " + "We lost our id (" + this.id + ")!");
              synchronized (this.idLock) {
                this.id = -1;
              }
            }

            @Override
            public void newId(int id, InstanceIdManager idm) {
              idm.report(idm, "Thread: " + "We got a new id of: " + id);
              synchronized (this.idLock) {
                this.id = id;
              }
            }

            @Override
            public void idManagementImpossible(InstanceIdManager idm) {
              idm.report(idm, "Thread: " + "Id management is no longer possible!");
            }

          }, NUM_IDS, IdContentionPolicy.BLOCKINDEFINITELY, start);
          try {
            // System.out.println("Starting new InstanceIdManager.");
            idm.start();
          } catch (Exception e) {
            e.printStackTrace();
            idm = null;
          }
          if (idm != null) {
            idms.add(idm);
          }
        }
      }

    };
    t.setDaemon(true);
    t.start();

    Thread r = new Thread() {
      private Random rand = new Random();

      @Override
      public void run() {
        try {
          Thread.sleep(TIMEOUT * 2);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        while (true) {
          if (idms.size() > 1) {
            InstanceIdManager inst = idms.remove(rand.nextInt(idms.size()));
            try {
              // System.out.println(Integer.toHexString(inst.hashCode()) + " " +
              // "Stopping");
              inst.stop();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
          try {
            Thread.sleep(TIMEOUT * 2);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

    };
    r.setDaemon(true);
    r.start();

    while (true) {
      Thread.sleep(1);
      InstanceIdManager idm = new InstanceIdManager("/sleet/datacenter/1", client, new ManagedInstanceIdUser() {
        private int id;
        private Object idLock = new Object();

        @Override
        public void idInvalidated(InstanceIdManager idm) {
          idm.report(idm, "Main: " + "We lost our id (" + this.id + ")!");
          synchronized (this.idLock) {
            this.id = -1;
          }
        }

        @Override
        public void newId(int id, InstanceIdManager idm) {
          idm.report(idm, "Main: " + "We got a new id of: " + id);
          synchronized (this.idLock) {
            this.id = id;
          }
        }

        @Override
        public void idManagementImpossible(InstanceIdManager idm) {
          idm.report(idm, "Id management is no longer possible!");
        }

      }, NUM_IDS, IdContentionPolicy.EXCEPTION, start);
      try {
        idm.start();
      } catch (Exception e) {
        if (!(e instanceof InstanceIdSpaceFullException)) {
          e.printStackTrace();
        }
        idm = null;
      }
      if (idm != null) {
        idms.add(idm);
      }
    }
  }
}
