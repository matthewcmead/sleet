package sleet.utils.zookeeper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import sleet.utils.curator.ZkMiniCluster;

public class TestInstanceIdManagerZooKeeper {
  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    ZkMiniCluster cluster = new ZkMiniCluster();
    File temp = File.createTempFile("instanceidmanager", "");
    temp.delete();
    temp.mkdir();
    System.out.println("zk temp path: " + temp.getAbsolutePath());
    cluster.startZooKeeper(temp.getAbsolutePath());
    String zkconn = cluster.getZkConnectionString();
    zkconn = "localhost:" + zkconn.substring(zkconn.lastIndexOf(':') + 1);
    System.out.println("ZK connection string: " + zkconn);
    ZooKeeper zooKeeper = new ZooKeeper(zkconn, 10000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    String path = "/id-manager";
    try {
      rmr(zooKeeper, path);
    } catch (Exception e) {
      // ignore
    }
    final InstanceIdManagerZooKeeper idManagerZooKeeper = new InstanceIdManagerZooKeeper(zooKeeper, path, 10);
    {
      int id1 = idManagerZooKeeper.tryToGetId(1000L);
      System.out.println(id1);
      idManagerZooKeeper.releaseId(id1);
    }
    {
      int id1 = idManagerZooKeeper.tryToGetId(1000L);
      System.out.println(id1);
      idManagerZooKeeper.releaseId(id1);
    }
    {
      int[] ids = new int[12];
      for (int i = 0; i < ids.length; i++) {
        ids[i] = idManagerZooKeeper.tryToGetId(1000L);
      }

      for (int i = 0; i < ids.length; i++) {
        System.out.println(ids[i]);
      }

      for (int i = 0; i < ids.length; i++) {
        idManagerZooKeeper.releaseId(ids[i]);
      }
    }

    {
      ExecutorService service = Executors.newCachedThreadPool();
      Random random = new Random();
      final AtomicInteger count = new AtomicInteger();
      int maxWorkers = 100;
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < maxWorkers; i++) {
        final int secondToWork = random.nextInt(2) + 1;
        final int index = i;
        futures.add(service.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            int id;
            int attempts = 0;
            do {
              if (attempts > 0) {
                Thread.sleep(100);
              }
              id = idManagerZooKeeper.tryToGetId(1000L);
              attempts++;
            } while (id < 0);
            try {
              System.out.println("[" + index + "] Working for [" + secondToWork + "] seconds with id [" + id + "]");
              Thread.sleep(TimeUnit.SECONDS.toMillis(secondToWork));
            } finally {
              System.out.println("[" + index + "] Releasing with id [" + id + "]");
              idManagerZooKeeper.releaseId(id);
              count.incrementAndGet();
            }
            return null;
          }
        }));
      }
      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          cause.printStackTrace();
        }
      }
      while (count.get() < maxWorkers) {
        System.out.println(count.get() + " " + maxWorkers);
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      }
      service.shutdown();
      service.awaitTermination(1, TimeUnit.DAYS);
    }

    zooKeeper.close();
  }

  private static void rmr(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
    List<String> children = zooKeeper.getChildren(path, false);
    for (String s : children) {
      rmr(zooKeeper, path + "/" + s);
    }
    zooKeeper.delete(path, -1);
  }
}
