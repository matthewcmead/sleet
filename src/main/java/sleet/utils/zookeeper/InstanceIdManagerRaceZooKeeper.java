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
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstanceIdManagerRaceZooKeeper extends InstanceIdManager {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceIdManagerRaceZooKeeper.class);

  private static final long _sessionValidityCacheTTL = 1000L;
  private static final long _checkThreadPeriod = 10000L;
  private static final int _nodeNameRadix = 16;
  private static final double _mustLockThreshold = 0.99;
  private static final double _noLongerMustLockThreshold = 0.85;

  private final int _maxInstances;
  private final ZooKeeper _zooKeeper;
  private final String _idPath;
  private final String _lockPath;
  private final String _mustLockPath;
  private final AtomicBoolean _sessionValid = new AtomicBoolean(true);
  private final int _stopLockingThreshold;
  private final int _startLockingThreshold;

  private int _id;
  private boolean _sessionValidCache = true;
  private String _assignedPath = null;
  private Stat _initialStat = null;
  private Thread _sessionChecker = null;
  private long _lastSessionCacheUpdate = -1;
  private AtomicBoolean _running = new AtomicBoolean(true);
  private String _lockNodePath = null;
  private String _prevLockNodePath = null;
  private final Object _lockLock = new Object();
  private final Watcher _lockWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      synchronized (_lockLock) {
        _lockLock.notify();
      }
    }
  };

  private final Object _idLock = new Object();
  private final Watcher _idWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      synchronized (_idLock) {
        _idLock.notify();
      }
    }
  };

  public InstanceIdManagerRaceZooKeeper(ZooKeeper zooKeeper, String path, int maxInstances) throws IOException {
    _maxInstances = maxInstances;
    _zooKeeper = zooKeeper;
    tryToCreate(path);
    _idPath = tryToCreate(path + "/id");
    _lockPath = tryToCreate(path + "/lock");
    _mustLockPath = path + "/mustLock";
    _startLockingThreshold = (int) Math.floor(_maxInstances * _mustLockThreshold);
    _stopLockingThreshold = (int) Math.floor(_maxInstances * _noLongerMustLockThreshold);
  }

  private String tryToCreate(String path) throws IOException {
    try {
      Stat stat = _zooKeeper.exists(path, false);
      if (stat == null) {
        _zooKeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (NodeExistsException e) {
      // another instance beat us to creating
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

  private Stat existsIfAvailable(String path) throws InterruptedException, IOException {
    try {
      return _zooKeeper.exists(path, false);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void deleteIfExists(String path, int version) throws InterruptedException, IOException {
    try {
      _zooKeeper.delete(path, version);
    } catch (KeeperException e) {
      if (e.code() != Code.NONODE) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public int tryToGetId(long millisToWait) throws IOException {
    LOG.debug(hashCode() + " Trying to get id with timeout " + millisToWait);

    try {
      final long initialTime = System.currentTimeMillis();
      final long endTime = millisToWait == -1 ? Long.MAX_VALUE : initialTime + millisToWait;

      boolean locked = false;
      Stat mustLockStat = null;

      while (endTime > System.currentTimeMillis()) {
        /**
         * Examine the mustLock path and see if someone has decided for us that
         * we must lock
         */
        mustLockStat = existsIfAvailable(_mustLockPath);

        if (mustLockStat != null) {
          if (!locked) {
            locked = tryToLock();
          }

          if (!locked) {
            /**
             * We didn't get the lock
             */

            /**
             * Check the number of children to see if we can race
             */
            Stat parentStat = null;
            try {
              parentStat = _zooKeeper.exists(_idPath, false);
            } catch (KeeperException e) {
              throw new IOException(e);
            }

            if (parentStat.getNumChildren() < _stopLockingThreshold) {
              /**
               * Threshold met such that we can ignore our lock and try to get
               * an id by racing
               */

              Stat stat = existsIfAvailable(_mustLockPath);
              if (stat != null) {
                LOG.debug(hashCode() + " Exiting locking mode.");

                deleteIfExists(_mustLockPath, -1);
              }

              int obtainedId = tryToRace(locked, endTime);
              if (obtainedId != -1) {
                _id = obtainedId;
                /**
                 * Start up background validity checker thread and remove our
                 * lock since we've found an id
                 */
                startSessionCheckerThread();
                unlock();
                return _id;
              }
            } else {
              /**
               * Must block waiting for lock
               */
              temporaryBlockOnLock();
            }
          } else {
            /**
             * We got the lock
             */
            int obtainedId = tryToRace(locked, endTime);
            if (obtainedId != -1) {
              _id = obtainedId;
              /**
               * Start up background validity checker thread and remove our lock
               * since we've found an id
               */
              startSessionCheckerThread();
              unlock();
              return _id;
            } else {
              temporaryBlockOnId();
            }
          }
        } else {
          /**
           * Noone has said we can't race
           */
          int obtainedId = tryToRace(locked, endTime);
          if (obtainedId != -1) {
            _id = obtainedId;
            /**
             * Start up background validity checker thread and remove our lock
             * since we've found an id
             */
            startSessionCheckerThread();
            unlock();
            return _id;
          }
        }
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    LOG.debug("Escaped loop to allocate an instance id.  Returning -1.");
    return -1;
  }

  private void temporaryBlockOnLock() throws IOException {
    if (_lockNodePath == null) {
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    } else {
      try {
        /**
         * Setup a watch on the previous lock holder's node
         */
        @SuppressWarnings("unused")
        Stat prevStat = _zooKeeper.exists(_prevLockNodePath, _lockWatcher);
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (KeeperException e) {
        // ignore in case it gets deleted while we're waiting
      }
      synchronized (_lockLock) {
        LOG.debug(hashCode() + " Waiting to look for our place in the lock again.");
        try {
          _lockLock.wait(TimeUnit.SECONDS.toMillis(1));
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    }
  }

  private void temporaryBlockOnId() throws IOException {
    try {
      /**
       * Setup a watch on the id parent node
       */
      @SuppressWarnings("unused")
      Stat idParentStat = _zooKeeper.exists(_idPath, _idWatcher);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }

    LOG.debug(hashCode() + " Waiting for an id slot to become free.");
    synchronized (_idLock) {
      try {
        _idLock.wait(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  private int tryToRace(boolean locked, long untilMillis) throws IOException {
    try {
      List<String> idstrs = _zooKeeper.getChildren(_idPath, false);

      if (idstrs.size() < _stopLockingThreshold) {
        Stat stat = existsIfAvailable(_mustLockPath);
        if (stat != null) {
          LOG.debug(hashCode() + " Exiting locking mode.");

          deleteIfExists(_mustLockPath, -1);
        }
      }

      int allocatedId = -1;
      boolean keepRacing = true;
      while (allocatedId == -1 && keepRacing && System.currentTimeMillis() < untilMillis) {
        idstrs = _zooKeeper.getChildren(_idPath, false);
        if (idstrs.size() >= _startLockingThreshold && !locked) {
          LOG.debug(hashCode() + " Entering locking mode.");
          tryToCreate(_mustLockPath);
          keepRacing = false;
        } else {
          int foundUnallocatedId = -1;
          if (idstrs.isEmpty()) {
            foundUnallocatedId = 0;
          } else {
            int[] ids = new int[idstrs.size()];
            int counter = 0;
            for (String id : idstrs) {
              ids[counter] = Integer.parseInt(getLeafValueForNode(id), _nodeNameRadix);
              counter++;
            }
            Arrays.sort(ids);
            int expectedId = 0;
            for (int i = 0; i < ids.length && foundUnallocatedId == -1; i++) {
              if (ids[i] != expectedId) {
                foundUnallocatedId = expectedId;
              }
              expectedId++;
            }
            if (foundUnallocatedId == -1 && ids.length < _maxInstances) {
              foundUnallocatedId = expectedId++;
            }
          }
          if (foundUnallocatedId != -1) {
            try {
              _assignedPath = _zooKeeper.create(_idPath + "/" + Integer.toString(foundUnallocatedId, _nodeNameRadix), null, Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL);
              Stat assignedPathStat = _zooKeeper.exists(_assignedPath, false);
              _initialStat = _zooKeeper.setData(_assignedPath, Integer.toString(foundUnallocatedId, _nodeNameRadix).getBytes(), assignedPathStat.getVersion());
              allocatedId = foundUnallocatedId;
            } catch (KeeperException e) {
              // ignore -- someone beat us to this slot
            }
          }
        }
      }
      return allocatedId;
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private static String getLeafValueForNode(String path) {
    int lastIndex = path.lastIndexOf("/");
    return lastIndex >= 0 ? path.substring(lastIndex + 1) : path;
  }

  private boolean tryToLock() throws IOException {
    try {
      if (_lockNodePath == null) {
        /**
         * We do not already have a node in the lock queue
         */
        _lockNodePath = _zooKeeper.create(_lockPath + "/lock_", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        LOG.debug(hashCode() + " Allocated lock entry in lock queue at path " + _lockNodePath);
      }
      int lockNumber = Integer.parseInt(_lockNodePath.substring(_lockNodePath.lastIndexOf('_') + 1));
      List<String> children = new ArrayList<String>(_zooKeeper.getChildren(_lockPath, false));
      if (children.size() < 1) {
        throw new IOException("Children of path [" + _lockPath + "] should never be 0.");
      }
      LOG.debug(hashCode() + " Looking through " + children.size() + " children to see if we own the lock.");
      TreeMap<Integer, String> sortedChildren = new TreeMap<Integer, String>();
      for (String child : children) {
        sortedChildren.put(Integer.parseInt(child.substring(child.lastIndexOf('_') + 1)), child);
      }
      int lockOwnerNumber = sortedChildren.keySet().iterator().next();
      LOG.debug(hashCode() + " First child number in lock dir is " + lockOwnerNumber + " and my number is " + lockNumber);
      if (lockNumber == lockOwnerNumber) {
        return true;
      }
      SortedMap<Integer, String> headMap = sortedChildren.headMap(lockNumber);
      _prevLockNodePath = _lockPath + "/" + headMap.get(headMap.lastKey());
      return false;
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void releaseId(int id) throws IOException {
    if (id != _id) {
      throw new IOException("Cannot release id=" + id + " when we own id=" + _id);
    }

    LOG.debug(hashCode() + " Releasing id=" + id);

    /**
     * Disrupt the session checker thread
     */
    _running.set(false);
    if (_sessionChecker != null) {
      _sessionChecker.interrupt();
    }
    try {
      _zooKeeper.delete(_assignedPath, -1);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
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
            Stat currentStat = _zooKeeper.exists(_assignedPath, false);
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

  private void unlock() throws IOException {
    if (_lockNodePath != null) {
      LOG.debug(hashCode() + " Unlocking.");
      try {
        _zooKeeper.delete(_lockNodePath, -1);
        _lockNodePath = null;
        _prevLockNodePath = null;
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (KeeperException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public boolean sessionValid(boolean allowValidityStateCaching) throws IOException {
    long now = System.currentTimeMillis();
    Exception caughtException = null;
    if (allowValidityStateCaching) {
      if (now - _lastSessionCacheUpdate > _sessionValidityCacheTTL) {
        _lastSessionCacheUpdate = now;
        _sessionValidCache = _sessionValid.get();
      }
    } else {
      /**
       * When we aren't allow to cache state, go check zookeeper ourselves
       */
      try {
        Stat currentStat = _zooKeeper.exists(_assignedPath, false);
        if (!_initialStat.equals(currentStat)) {
          _sessionValid.set(false);
        }
      } catch (Exception e) {
        _sessionValid.set(false);
        caughtException = e;
      }
      _lastSessionCacheUpdate = now;
      _sessionValidCache = _sessionValid.get();
    }
    if (caughtException != null) {
      throw new IOException(caughtException);
    }
    return _sessionValidCache;
  }
}
