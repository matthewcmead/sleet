package sleet;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import sleet.generators.time.TimeDependentSequenceIdGenerator;
import sleet.utils.zookeeper.ZkMiniCluster;

public class TestSleetIdGenerator {
  public static final int NUM_INSTANCES = 100;

  public static void main(String[] args) {
    ZkMiniCluster cluster = null;
    try {
      cluster = new ZkMiniCluster();
      File temp = File.createTempFile("instanceidmanager", "");
      temp.delete();
      temp.mkdir();
      System.out.println("zk temp path: " + temp.getAbsolutePath());
      cluster.startZooKeeper(temp.getAbsolutePath());
      String zkconn = cluster.getZkConnectionString();
      zkconn = "localhost:" + zkconn.substring(zkconn.lastIndexOf(':') + 1);
      System.out.println("ZK connection string: " + zkconn);

      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      Thread[] threads = new Thread[NUM_INSTANCES];
      final Object lock = new Object();
      for (int instance = 0; instance < NUM_INSTANCES; instance++) {
        Thread t = new Thread() {

          @Override
          public void run() {
            try {
              SleetIdGenerator gen = new SleetIdGenerator();
              Properties props = new Properties();
              InputStream is = gen.getClass().getResourceAsStream("sleet.properties");
              props.load(is);
              is.close();
              gen.beginIdSession(props);
              int loops = 100000;
              long start = System.currentTimeMillis();
              long maxseq = (1L << Integer.parseInt(props.getProperty(TimeDependentSequenceIdGenerator.BITS_IN_SEQUENCE_KEY))) - 1L;
              long highest = -1;
              for (int i = 0; i < loops; i++) {
                // System.out.println(SleetIdGenerator.paddedBinary(gen.getId().getId()));
                long id = gen.getId().getId();
                long seq = id & maxseq;
                if (seq > highest) {
                  highest = seq;
                }
              }
              gen.endIdSession();
              long duration = System.currentTimeMillis() - start;
              System.out.println("Duration for " + loops + " ids: " + duration + "ms");
              System.out.println("Mean ids per ms: " + loops / duration);
              System.out.println("Max sequence: " + highest);
              synchronized (lock) {
                lock.notify();
              }
            } catch (Exception e) {
              e.printStackTrace();
              return;
            }
          }

        };
        threads[instance] = t;
        t.start();
      }
      boolean finished = false;
      while (!finished) {
        synchronized (lock) {
          lock.wait(TimeUnit.SECONDS.toMillis(1));
        }
        boolean alive = false;
        for (int instance = 0; instance < threads.length && !alive; instance++) {
          if (threads[instance].isAlive()) {
            alive = true;
          }
        }
        finished = !alive;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return;
    } finally {
      if (cluster != null) {
        cluster.shutdownZooKeeper();
      }
    }
  }
}
