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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class InstanceIdManagerZooKeeper extends InstanceIdManager {

  private static final long _sessionValidityCacheTTL = 1000L;
  private static final long _waitForIdQueueTimeout = 1000L;
  private static final long _checkThreadPeriod = 10000L;

  private final int _maxInstances;
  private final ZooKeeper _zooKeeper;
  private final String _idPath;
  private final String _lockPath;
  private final AtomicBoolean _sessionValid = new AtomicBoolean(true);
  private boolean _sessionValidCache = true;
  private String _assignedNode = null;
  private Stat _initialStat = null;
  private Thread _sessionChecker = null;
  private long _lastSessionCacheUpdate = -1;
  private AtomicBoolean _running = new AtomicBoolean(true);

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
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return path;
  }

  @Override
  public int getMaxNumberOfInstances() {
    return _maxInstances;
  }

  @Override
  public int tryToGetId(long millisToWait) throws IOException {
    final long initialTime = System.currentTimeMillis();
    final Object watchLock = new Object();
    final Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        synchronized (watchLock) {
          watchLock.notify();
        }
      }
    };
    String newPath = null;
    try {
      newPath = _zooKeeper.create(_idPath + "/id_", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    String lock = null;
    try {
      int count = -1;
      while (count == -1 || count >= _maxInstances) {
        lock = lock();
        List<String> children = _zooKeeper.getChildren(_idPath, watcher);
        int[] childNumbers = new int[children.size()];
        int childCounter = 0;
        for (String child : children) {
          childNumbers[childCounter++] = Integer.parseInt(child.substring(child.lastIndexOf('_') + 1));
        }
        Arrays.sort(childNumbers);
        int myNumber = Integer.parseInt(newPath.substring(newPath.lastIndexOf('_') + 1));
        String newNode = newPath.substring(newPath.lastIndexOf('/') + 1);
        for (int childNumber : childNumbers) {
          if (myNumber == childNumber) {
            break;
          }
          count++;
        }
        // new node is in the top children, assign a new id.
        if (count < _maxInstances) {
          return assignNode(newNode);
        } else {
          if (System.currentTimeMillis() - initialTime >= millisToWait) {
            _zooKeeper.delete(newPath, -1);
            return -1;
          }
          unlock(lock);
          lock = null;
          synchronized (watchLock) {
            watchLock.wait(_waitForIdQueueTimeout);
          }
        }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      if (lock != null) {
        unlock(lock);
      }
    }
    return -1;
  }

  @Override
  public void releaseId(int id) throws IOException {
    final String lock = lock();
    _running.set(false);
    if (_sessionChecker != null) {
      _sessionChecker.interrupt();
    }
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
      } catch (KeeperException e) {
        throw new IOException(e);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    } finally {
      unlock(lock);
    }
  }

  private int assignNode(String newNode) throws KeeperException, InterruptedException, IOException {
    List<String> children = new ArrayList<String>(_zooKeeper.getChildren(_idPath, false));
    TreeMap<Integer, String> sortedChildren = new TreeMap<Integer, String>();
    for (String child : children) {
      sortedChildren.put(Integer.parseInt(child.substring(child.lastIndexOf('_') + 1)), child);
    }
    int count = 0;
    int newNodeIndex = -1;
    Stat newNodeStat = null;
    BitSet bitSet = new BitSet(_maxInstances);
    for (Map.Entry<Integer, String> entry : sortedChildren.entrySet()) {
      if (count >= _maxInstances) {
        break;
      }

      String path = _idPath + "/" + entry.getValue();
      Stat stat = _zooKeeper.exists(path, false);
      if (stat == null) {
        // This should never happen because the lock prevents it.
        throw new IOException("This should never happen because the lock prevents it.");
      }
      if (entry.getValue().equals(newNode)) {
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
    _assignedNode = _idPath + "/" + newNode;
    _initialStat = _zooKeeper.setData(_assignedNode, Integer.toString(nextClearBit).getBytes(), newNodeStat.getVersion());
    /**
     * Sanity check to ensure there is only one node with our id
     */
    children = new ArrayList<String>(_zooKeeper.getChildren(_idPath, false));
    int idFoundCount = 0;
    for (String s : children) {
      String path = _idPath + "/" + s;
      Stat stat = _zooKeeper.exists(path, false);
      if (stat == null) {
        // This should never happen because the lock prevents it.
        throw new IOException("This should never happen because the lock prevents it.");
      }
      byte[] data = _zooKeeper.getData(path, false, stat);
      if (data != null) {
        if (nextClearBit == Integer.parseInt(new String(data))) {
          idFoundCount++;
        }
      }
    }
    if (idFoundCount != 1) {
      throw new IOException("When allocating instance number " + nextClearBit + " in path " + newNode + ", found " + idFoundCount + " instance(s) with that instance number.");
    }
    startSessionCheckerThread();
    return nextClearBit;
  }

  private void startSessionCheckerThread() {
    _sessionChecker = new Thread() {
      @Override
      public void run() {
        while (_running.get()) {
          if (Thread.interrupted()) {
            return;
          }
          try {
            Thread.sleep(_checkThreadPeriod);
          } catch (InterruptedException e) {
            return;
          }
          try {
            Stat currentStat = _zooKeeper.exists(_assignedNode, false);
            if (!_initialStat.equals(currentStat)) {
              _sessionValid.set(false);
              return;
            }
          } catch (Exception e) {
            _sessionValid.set(false);
            return;
          }
        }
      }
    };
    _sessionChecker.setDaemon(true);
    _sessionChecker.start();
  }

  private void unlock(String lock) throws IOException {
    try {
      _zooKeeper.delete(_lockPath + "/" + lock, -1);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private String lock() throws IOException {
    try {
      String path = _zooKeeper.create(_lockPath + "/lock_", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      String lockNode = path.substring(path.lastIndexOf('/') + 1);
      int lockNumber = Integer.parseInt(lockNode.substring(lockNode.lastIndexOf('_') + 1));
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
        List<String> children = new ArrayList<String>(_zooKeeper.getChildren(_lockPath, watcher));
        if (children.size() < 1) {
          throw new IOException("Children of path [" + _lockPath + "] should never be 0.");
        }
        int[] childNumbers = new int[children.size()];
        int childCounter = 0;
        for (String child : children) {
          childNumbers[childCounter++] = Integer.parseInt(child.substring(child.lastIndexOf('_') + 1));
        }
        Arrays.sort(childNumbers);
        int lockOwnerNumber = childNumbers[0];
        if (lockNumber == lockOwnerNumber) {
          return lockNode;
        }
        synchronized (lock) {
          lock.wait(TimeUnit.SECONDS.toMillis(1));
        }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean sessionValid(boolean allowValidityStateCaching) {
    long now = System.currentTimeMillis();
    if (!allowValidityStateCaching || (now - _lastSessionCacheUpdate > _sessionValidityCacheTTL)) {
      _lastSessionCacheUpdate = now;
      _sessionValidCache = _sessionValid.get();
    }

    return _sessionValidCache;
  }
}
