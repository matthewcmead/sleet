/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleet.utils.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class InstanceIdManagerZooKeeper extends InstanceIdManager {

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    ZooKeeper zooKeeper = new ZooKeeper("localhost", 10000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    String path = "/id-manager";
    rmr(zooKeeper, path);
    final InstanceIdManagerZooKeeper idManagerZooKeeper = new InstanceIdManagerZooKeeper(zooKeeper, path, 10);
    {
      int id1 = idManagerZooKeeper.tryToGetId();
      System.out.println(id1);
      idManagerZooKeeper.releaseId(id1);
    }
    {
      int id1 = idManagerZooKeeper.tryToGetId();
      System.out.println(id1);
      idManagerZooKeeper.releaseId(id1);
    }
    {
      int[] ids = new int[12];
      for (int i = 0; i < ids.length; i++) {
        ids[i] = idManagerZooKeeper.tryToGetId();
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
              id = idManagerZooKeeper.tryToGetId();
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

  private final int _maxInstances;
  private final ZooKeeper _zooKeeper;
  private final String _idPath;
  private final String _lockPath;

  public InstanceIdManagerZooKeeper(ZooKeeper zooKeeper, String path, int maxInstances) throws IOException {
    _maxInstances = maxInstances;
    _zooKeeper = zooKeeper;
    tryToCreate(path);
    _idPath = tryToCreate(path + "/id");
    _lockPath = tryToCreate(path + "/lock");
  }

  private String tryToCreate(String path) throws IOException {
    try {
      Stat stat = _zooKeeper.exists(path, false);
      if (stat == null) {
        _zooKeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
    return path;
  }

  @Override
  public int getMaxNumberOfInstances() {
    return _maxInstances;
  }

  @Override
  public int tryToGetId() throws IOException {
    final String lock = lock();
    try {
      String newPath = _zooKeeper.create(_idPath + "/id_", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      List<String> children = new ArrayList<>(_zooKeeper.getChildren(_idPath, false));
      Collections.sort(children);
      String newNode = newPath.substring(newPath.lastIndexOf('/') + 1);
      int count = 0;
      for (String s : children) {
        if (newNode.equals(s)) {
          break;
        }
        count++;
      }
      // new node is in the top children, assign a new id.
      if (count < _maxInstances) {
        return assignNode(newNode);
      }
      _zooKeeper.delete(newPath, -1);
      return -1;
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    } finally {
      unlock(lock);
    }
  }

  @Override
  public void releaseId(int id) throws IOException {
    final String lock = lock();
    try {
      try {
        List<String> children = _zooKeeper.getChildren(_idPath, false);
        for (String s : children) {
          String path = _idPath + "/" + s;
          Stat stat = _zooKeeper.exists(path, false);
          if (stat == null) {
            // This should never happen because the lock prevents it.
            throw new IOException("This should never happen because the lock prevents it.");
          }
          byte[] data = _zooKeeper.getData(path, false, stat);
          if (data == null) {
            // Nodes that have not been assigned.
            continue;
          }
          int storedId = Integer.parseInt(new String(data));
          if (id == storedId) {
            _zooKeeper.delete(path, stat.getVersion());
            return;
          }
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    } finally {
      unlock(lock);
    }
  }

  private int assignNode(String newNode) throws KeeperException, InterruptedException, IOException {
    List<String> children = new ArrayList<>(_zooKeeper.getChildren(_idPath, false));
    int count = 0;
    int newNodeIndex = -1;
    Stat newNodeStat = null;
    BitSet bitSet = new BitSet(_maxInstances);
    for (String s : children) {
      if (count >= _maxInstances) {
        break;
      }

      String path = _idPath + "/" + s;
      Stat stat = _zooKeeper.exists(path, false);
      if (stat == null) {
        // This should never happen because the lock prevents it.
        throw new IOException("This should never happen because the lock prevents it.");
      }
      if (s.equals(newNode)) {
        newNodeIndex = count;
        newNodeStat = stat;
      }
      byte[] data = _zooKeeper.getData(path, false, stat);
      if (data != null) {
        bitSet.set(Integer.parseInt(new String(data)));
      }
      count++;
    }
    if (newNodeIndex == -1) {
      // Someone else grabbed the lock before us.
      _zooKeeper.delete(_idPath + "/" + newNode, -1);
      return -1;
    }
    int nextClearBit = bitSet.nextClearBit(0);
    if (nextClearBit < 0 || nextClearBit >= _maxInstances) {
      _zooKeeper.delete(_idPath + "/" + newNode, -1);
      return -1;
    }
    _zooKeeper.setData(_idPath + "/" + newNode, Integer.toString(nextClearBit).getBytes(), newNodeStat.getVersion());
    return nextClearBit;
  }

  private void unlock(String lock) throws IOException {
    try {
      _zooKeeper.delete(_lockPath + "/" + lock, -1);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  private String lock() throws IOException {
    try {
      String path = _zooKeeper.create(_lockPath + "/lock-", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      String lockNode = path.substring(path.lastIndexOf('/') + 1);
      final Object lock = new Object();
      Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          synchronized (lock) {
            lock.notifyAll();
          }
        }
      };
      while (true) {
        List<String> children = new ArrayList<>(_zooKeeper.getChildren(_lockPath, watcher));
        Collections.sort(children);
        if (children.size() < 1) {
          throw new IOException("Children of path [" + _lockPath + "] should never be 0.");
        }
        String lockOwner = children.get(0);
        if (lockNode.equals(lockOwner)) {
          return lockNode;
        }
        synchronized (lock) {
          lock.wait(TimeUnit.SECONDS.toMillis(1));
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }
}
