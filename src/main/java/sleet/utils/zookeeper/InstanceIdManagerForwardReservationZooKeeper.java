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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.beanutils.ConvertUtils;
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

public class InstanceIdManagerForwardReservationZooKeeper extends InstanceIdManager {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceIdManagerForwardReservationZooKeeper.class);

  /**
   * These credentials are used to add authentication to the zookeeper session
   * and to apply ACLs to the permanent nodes used by this implementation to
   * reserve an id generation instance. If you are cherrypicking these values
   * out of this source code to do any manual manipulation of the zookeeper data
   * stored for sleet, make very sure you know exactly what you are doing or you
   * could very easily cause the distributed system to generate duplicate ids,
   * which is very very bad.
   */
  private static final String zkAuthUser = "sleet";
  private static final String zkAuthPassword = "RwcH9O4LjyfqnQOlSKVVk2ByIbTagMJdzHyT4n3Vpbs";
  private static final byte[] zkAddAuthBytes = (zkAuthUser + ":" + zkAuthPassword).getBytes();

  /**
   * 
   */

  public static String zkFwdReservationReservationCheckPeriod = "zk.instmgr.fwdreservation.reservation.check.period.ms";
  public static String zkFwdReservationMustLockThreshold = "zk.instmgr.fwdreservation.must.lock.threshold.fraction";
  public static String zkFwdReservationStopLockThreshold = "zk.instmgr.fwdreservation.stop.lock.threshold.fraction";
  public static String zkFwdReservationMaxZkClockOffset = "zk.instmgr.fwdreservation.max.zk.clock.offset.ms";
  public static String zkFwdReservationReservationLength = "zk.instmgr.fwdreservation.reservation.length.ms";
  public static String zkFwdReservationReservationReapGrace = "zk.instmgr.fwdreservation.reservation.reap.graceperiod.ms";

  private long _checkThreadPeriod = 10000L;
  private int _nodeNameRadix = 16;
  private double _mustLockThreshold = 0.99;
  private double _noLongerMustLockThreshold = 0.85;
  private long _maxZkClockOffset = 10000L;
  private long _forwardReservationInterval = 60000L;
  private long _reservationReapGracePeriod = 120000L;

  private final int _maxInstances;
  private final ZooKeeper _zooKeeper;
  private final String _idPath;
  private final String _procPath;
  private final String _lockPath;
  private final String _mustLockPath;
  private final int _stopLockingThreshold;
  private final int _startLockingThreshold;

  private long _zkClockOffset;

  private final Random _random;

  private int _id;
  private String _assignedPath = null;
  private Stat _assignedNodeLastStat = null;
  private Thread _sessionChecker = null;
  private long _forwardReservation;
  private AtomicBoolean _running = new AtomicBoolean(true);
  private String _lockNodePath = null;
  private String _prevLockNodePath = null;
  private String _procNodePath = null;
  private Stat _procNodeInitialStat;
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

  public InstanceIdManagerForwardReservationZooKeeper(ZooKeeper zooKeeper, String path, int maxInstances, Properties config) throws IOException {
    _maxInstances = maxInstances;
    _zooKeeper = zooKeeper;
    tryToCreate(path);
    _idPath = tryToCreate(path + "/id");
    _procPath = tryToCreate(path + "/proc");
    _lockPath = tryToCreate(path + "/lock");
    _zooKeeper.addAuthInfo("digest", zkAddAuthBytes);
    _mustLockPath = path + "/mustLock";
    _startLockingThreshold = (int) Math.floor(_maxInstances * _mustLockThreshold);
    _stopLockingThreshold = (int) Math.floor(_maxInstances * _noLongerMustLockThreshold);
    _random = new Random(System.currentTimeMillis() + hashCode());
    String val;
    val = config.getProperty(zkFwdReservationReservationCheckPeriod);
    if (val != null) {
      Object o = ConvertUtils.convert(val, Long.class);
      if (o instanceof Long) {
        _checkThreadPeriod = (Long) o;
      } else {
        throw new IOException("Could not coerce " + zkFwdReservationReservationCheckPeriod + " value \"" + val + "\" to a long.");
      }
    }
    val = config.getProperty(zkFwdReservationMustLockThreshold);
    if (val != null) {
      Object o = ConvertUtils.convert(val, Double.class);
      if (o instanceof Double) {
        _mustLockThreshold = (Double) o;
      } else {
        throw new IOException("Could not coerce " + zkFwdReservationMustLockThreshold + " value \"" + val + "\" to a double.");
      }
    }
    val = config.getProperty(zkFwdReservationStopLockThreshold);
    if (val != null) {
      Object o = ConvertUtils.convert(val, Double.class);
      if (o instanceof Double) {
        _noLongerMustLockThreshold = (Double) o;
      } else {
        throw new IOException("Could not coerce " + zkFwdReservationStopLockThreshold + " value \"" + val + "\" to a double.");
      }
    }
    val = config.getProperty(zkFwdReservationMaxZkClockOffset);
    if (val != null) {
      Object o = ConvertUtils.convert(val, Long.class);
      if (o instanceof Long) {
        _maxZkClockOffset = (Long) o;
      } else {
        throw new IOException("Could not coerce " + zkFwdReservationMaxZkClockOffset + " value \"" + val + "\" to a long.");
      }
    }
    val = config.getProperty(zkFwdReservationReservationLength);
    if (val != null) {
      Object o = ConvertUtils.convert(val, Long.class);
      if (o instanceof Long) {
        _forwardReservationInterval = (Long) o;
      } else {
        throw new IOException("Could not coerce " + zkFwdReservationReservationLength + " value \"" + val + "\" to a long.");
      }
    }
    val = config.getProperty(zkFwdReservationReservationReapGrace);
    if (val != null) {
      Object o = ConvertUtils.convert(val, Long.class);
      if (o instanceof Long) {
        _reservationReapGracePeriod = (Long) o;
      } else {
        throw new IOException("Could not coerce " + zkFwdReservationReservationReapGrace + " value \"" + val + "\" to a long.");
      }
    }
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
    if (LOG.isDebugEnabled()) {
      LOG.debug(hashCode() + " Trying to get id with timeout " + millisToWait);
    }

    String procPathPrefix = _procPath + "/proc_";
    try {
      long mymillis = System.currentTimeMillis();
      _procNodePath = _zooKeeper.create(_procPath + "/proc_", null, Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL_SEQUENTIAL);
      _procNodeInitialStat = _zooKeeper.exists(_procNodePath, false);
      long zkmillis = _procNodeInitialStat.getCtime();
      if (Math.abs(mymillis - zkmillis) > _maxZkClockOffset) {
        throw new IOException("ZooKeeper clock and my clock diverge by " + _maxZkClockOffset + " ms.  Aborting.");
      }
      _zkClockOffset = zkmillis - mymillis;
    } catch (InterruptedException e) {
      throw new IOException("Interrupted attempting to create proc path.", e);
    } catch (KeeperException e) {
      throw new IOException("Unable to create ephemeral sequential node with proc path prefix \"" + procPathPrefix + "\".");
    }

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
                if (LOG.isDebugEnabled()) {
                  LOG.debug(hashCode() + " Exiting locking mode.");
                }

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
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} Escaped loop to allocate an instance id.  Returning -1.", hashCode());
    }
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
        if (LOG.isDebugEnabled()) {
          LOG.debug(hashCode() + " Waiting to look for our place in the lock again.");
        }
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

    if (LOG.isDebugEnabled()) {
      LOG.debug(hashCode() + " Waiting for an id slot to become free.");
    }
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
          if (LOG.isDebugEnabled()) {
            LOG.debug(hashCode() + " Exiting locking mode.");
          }
          deleteIfExists(_mustLockPath, -1);
        }
      }

      int allocatedId = -1;
      boolean keepRacing = true;
      while (allocatedId == -1 && keepRacing && System.currentTimeMillis() < untilMillis) {
        idstrs = _zooKeeper.getChildren(_idPath, false);
        if (idstrs.size() >= _startLockingThreshold && !locked) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(hashCode() + " Entering locking mode.");
          }
          tryToCreate(_mustLockPath);
          keepRacing = false;
        } else {
          int foundUnallocatedId = -1;
          int[] ids = new int[idstrs.size()];
          if (idstrs.isEmpty()) {
            foundUnallocatedId = 0;
          } else {
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
              assignNode(foundUnallocatedId);
              allocatedId = foundUnallocatedId;
            } catch (KeeperException e) {
              /**
               * someone beat us to this slot, pick a random slot between
               * collision and max slot, attempt to allocate, otherwise repeat
               */
              if (LOG.isDebugEnabled()) {
                LOG.debug("{} Collided on sort-chosen slot {}.", hashCode(), foundUnallocatedId);
              }
              int randomId = -1;
              try {
                randomId = foundUnallocatedId + _random.nextInt((_maxInstances - 1) - foundUnallocatedId);
                assignNode(randomId);
                allocatedId = randomId;
                if (LOG.isDebugEnabled()) {
                  LOG.debug("{} Took random slot {} instead of resort after collide.", hashCode(), randomId);
                }
              } catch (KeeperException e2) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("{} Collided on random slot {}.", hashCode(), randomId);
                } // ignore -- someone beat us to this slot
              }
            }
          }
          if (allocatedId == -1 && locked) {
            /**
             * ensure we only take one shot to reap the id before waiting
             */
            keepRacing = false;
            /**
             * we didn't find an id but we have the lock, let's see if we can
             * reap an id
             */
            for (int idindex = 0; idindex < ids.length && allocatedId == -1; idindex++) {
              int candidateId = ids[idindex];
              try {
                String candidatePath = _idPath + "/" + Integer.toString(candidateId, _nodeNameRadix);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("{} Examinining candidatePath {} to determine if it can be reaped.", hashCode(), candidatePath);
                }
                byte[] data = _zooKeeper.getData(candidatePath, false, null);
                ReservationInfo reservation = getReservationFromBytes(data);
                long now = System.currentTimeMillis();
                if ((reservation.reservationEndpoint - _zkClockOffset + _reservationReapGracePeriod) < now) {
                  if (_zooKeeper.exists(reservation.procNodePath, false) == null) {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("{} Node: {}; lease: {}; now: {}; now-lease: {}; gracePeriod: {}", new Object[] { hashCode(), candidatePath, (Long) (reservation.reservationEndpoint - _zkClockOffset),
                          now, (Long) (now - (reservation.reservationEndpoint - _zkClockOffset)), (Long) _reservationReapGracePeriod });
                    }
                    /**
                     * The reservation is old enough in zookeeper time and there
                     * is no ephemeral node existing for this reservation, so
                     * reap it.
                     */
                    _zooKeeper.delete(candidatePath, -1);
                    assignNode(candidateId);
                    allocatedId = candidateId;
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("{} Reaped {} and got allocatedId={}", new Object[] { hashCode(), candidatePath, allocatedId });
                    }
                  } else {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("{} Could not reap {} because its procNode {} was still in existence.", new Object[] { hashCode(), candidatePath, reservation.procNodePath });
                    }
                  }
                } else {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("{} Node: {}; lease: {}; now: {}; now-lease: {}; gracePeriod: {}", new Object[] { hashCode(), candidatePath, (Long) (reservation.reservationEndpoint - _zkClockOffset),
                        now, (Long) (now - (reservation.reservationEndpoint - _zkClockOffset)), (Long) _reservationReapGracePeriod });
                  }
                }
              } catch (KeeperException e) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(hashCode() + " Had an issue with reaping ids using ZooKeeper", e);
                } // ignore -- don't try to reap this spot
              }
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

  private void assignNode(int id) throws KeeperException, InterruptedException, IOException {
    _assignedPath = _zooKeeper.create(_idPath + "/" + Integer.toString(id, _nodeNameRadix), null, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
    long reservation = System.currentTimeMillis() + _forwardReservationInterval;
    setReservation(reservation);
  }

  private void setReservation(long reservation) throws KeeperException, InterruptedException, IOException {
    long now;
    if (_assignedNodeLastStat != null) {
      now = System.currentTimeMillis();
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} Existing _assignedNodeLastStat: {}", hashCode(), _assignedNodeLastStat);
      }
      _assignedNodeLastStat = _zooKeeper.setData(_assignedPath, getBytesForReservation(new ReservationInfo(reservation + _zkClockOffset, _procNodePath)), _assignedNodeLastStat.getVersion());
    } else {
      now = System.currentTimeMillis();
      _assignedNodeLastStat = _zooKeeper.setData(_assignedPath, getBytesForReservation(new ReservationInfo(reservation + _zkClockOffset, _procNodePath)), -1);
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} Assigned _assignedNodeLastStat: {}", hashCode(), _assignedNodeLastStat);
      }
    }
    _forwardReservation = reservation;
    _zkClockOffset = _assignedNodeLastStat.getMtime() - now;
  }

  private static byte[] getBytesForReservation(ReservationInfo reservation) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    dos.writeLong(reservation.reservationEndpoint);
    dos.writeUTF(reservation.procNodePath);
    dos.close();
    bos.close();
    return bos.toByteArray();
  }

  private static ReservationInfo getReservationFromBytes(byte[] bytes) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bis);
    long reservationEndpoint = dis.readLong();
    String procNodePath = dis.readUTF();
    return new ReservationInfo(reservationEndpoint, procNodePath);
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
        _lockNodePath = _zooKeeper.create(_lockPath + "/lock_", null, Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL_SEQUENTIAL);
        if (LOG.isDebugEnabled()) {
          LOG.debug(hashCode() + " Allocated lock entry in lock queue at path " + _lockNodePath);
        }
      }
      int lockNumber = Integer.parseInt(_lockNodePath.substring(_lockNodePath.lastIndexOf('_') + 1));
      List<String> children = new ArrayList<String>(_zooKeeper.getChildren(_lockPath, false));
      if (children.size() < 1) {
        throw new IOException("Children of path [" + _lockPath + "] should never be 0.");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(hashCode() + " Looking through " + children.size() + " children to see if we own the lock.");
      }
      TreeMap<Integer, String> sortedChildren = new TreeMap<Integer, String>();
      for (String child : children) {
        sortedChildren.put(Integer.parseInt(child.substring(child.lastIndexOf('_') + 1)), child);
      }
      int lockOwnerNumber = sortedChildren.keySet().iterator().next();
      if (LOG.isDebugEnabled()) {
        LOG.debug(hashCode() + " First child number in lock dir is " + lockOwnerNumber + " and my number is " + lockNumber);
      }
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

    if (LOG.isDebugEnabled()) {
      LOG.debug(hashCode() + " Releasing id=" + id);
    }
    /**
     * Disrupt the session checker thread
     */
    _running.set(false);
    if (_sessionChecker != null) {
      _sessionChecker.interrupt();
    }
    try {
      _zooKeeper.delete(_assignedPath, -1);
      _zooKeeper.delete(_procNodePath, -1);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void startSessionCheckerThread() {
    /**
     * MCM when we are <= 10s of our reservation, update the reservation,
     * validate that our initial stats are the same
     */
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
            long now = System.currentTimeMillis();
            if (now > _forwardReservation) {
              /**
               * the main thread must get a new session once we pass our
               * reservation, do nothing and let it happen
               */
              return;
            } else if ((_forwardReservation - now) <= _checkThreadPeriod) {
              /**
               * Check validity of stats
               */
              Stat currentStat = _zooKeeper.exists(_assignedPath, false);
              Stat currentProcNodeStat = _zooKeeper.exists(_procNodePath, false);
              if (!_assignedNodeLastStat.equals(currentStat) || !_procNodeInitialStat.equals(currentProcNodeStat)) {
                /**
                 * the main thread must get a new session once we pass our
                 * reservation, do nothing and let it happen
                 */
                return;
              } else {
                /**
                 * Update reservation out another period
                 */
                long reservation = System.currentTimeMillis() + _forwardReservationInterval;
                setReservation(reservation);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("{} Just increased reservation to zk time: {}", hashCode(), new Date(reservation));
                }
              }
            }
          } catch (Exception e) {
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
      if (LOG.isDebugEnabled()) {
        LOG.debug(hashCode() + " Unlocking.");
      }
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
    return System.currentTimeMillis() <= _forwardReservation;
  }

  @Override
  public int getCurrentId() throws IOException {
    return _id;
  }

  private static class ReservationInfo {
    long reservationEndpoint;
    String procNodePath;

    public ReservationInfo(long reservationEndpoint, String procNodePath) {
      super();
      this.reservationEndpoint = reservationEndpoint;
      this.procNodePath = procNodePath;
    }
  }
}
