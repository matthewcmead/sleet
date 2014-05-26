package sleet;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import sleet.utils.curator.ZkMiniCluster;

public class TestSleetIdGenerator {
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

      SleetIdGenerator gen = new SleetIdGenerator();
      Properties props = new Properties();
      InputStream is = gen.getClass().getResourceAsStream("sleet.properties");
      props.load(is);
      is.close();
      gen.beginIdSession(props);
      int loops = 1000000;
      long start = System.currentTimeMillis();
      long maxseq = 255;
      long highest = -1;
      for (int i = 0; i < loops; i++) {
        // System.out.println(SleetIdGenerator.paddedBinary(gen.getId().getId()));
        long id = gen.getId().getId();
        long seq = id & maxseq;
        if (seq > highest) {
          highest = seq;
        }
      }
      long duration = System.currentTimeMillis() - start;
      System.out.println("Duration for " + loops + " ids: " + duration + "ms");
      System.out.println("Mean ids per ms: " + loops / duration);
      System.out.println("Max sequence: " + highest);
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
