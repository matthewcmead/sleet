package sleet.utils.curator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * A class to manage (over zookeeper) the act of allocating a currently unique
 * instance id within a single zookeeper quorum.
 * 
 * @author mmead
 * 
 */

public class InstanceIdManager implements CuratorListener {
  private static final String LOCK_PATH_EXTENSION = "_lock";
  private static final String LOCK_NAME = "idmgrlock";
  private static final int CHILD_NODE_NAME_RADIX = 16;
  private static final long TIMEOUT = 1000;
  private CuratorFramework client = null;
  private final String instanceIdParentPath;
  private final ManagedInstanceIdUser iduser;
  private final int maximumId;
  private int currentId;
  private String currentPath = null;
  private final IdContentionPolicy allocationPolicy;
  private boolean ignoreWatches = false;
  private final String instanceIdParentLockPath;
  private final Object lock = new Object();
  private final Object fullLock = new Object();
  private String currentLockPath = null;
  private byte[] currentNodeUuid = null;
  private final long epoch;

  public static enum IdContentionPolicy {
    BLOCKINDEFINITELY, EXCEPTION
  }

  public InstanceIdManager(String instanceIdParentPath, CuratorFramework client, ManagedInstanceIdUser iduser, int maximumId,
      IdContentionPolicy allocationPolicy, long epoch) {
    this.instanceIdParentPath = instanceIdParentPath;
    this.instanceIdParentLockPath = this.instanceIdParentPath + LOCK_PATH_EXTENSION;
    this.client = client;
    this.iduser = iduser;
    this.maximumId = maximumId;
    this.allocationPolicy = allocationPolicy;
    this.epoch = epoch;
  }

  void report(InstanceIdManager inst, String message) {
    System.out.println((System.currentTimeMillis() - this.epoch) + " " + Integer.toHexString(inst.hashCode()) + " " + message);
  }

  private void makePath(String path) throws Exception {
    try {
      client.create().creatingParentsIfNeeded().forPath(path);
    } catch (NodeExistsException e) {
      // ignore
    }
  }

  private void makeParent() throws Exception {
    makePath(this.instanceIdParentPath);
  }

  private void makeLockParent() throws Exception {
    makePath(this.instanceIdParentLockPath);
  }

  public void start() throws Exception {
    makeParent();
    this.client.getCuratorListenable().addListener(this);
    this.client.getData().watched().forPath(this.instanceIdParentPath);
    this.client.getChildren().watched().forPath(this.instanceIdParentPath);
    getNewId();
  }

  public void stop() throws Exception {
    if (this.currentPath != null) {
      this.ignoreWatches = true;
      report(this, "Stopping...");
      delete(this.currentPath, true);
    }
  }

  private void invalidate() {
    this.currentId = -1;
    this.currentPath = null;
    this.currentLockPath = null;
    this.currentNodeUuid = null;
    this.iduser.idInvalidated(this);
  }

  private void delete(String path, boolean print) throws Exception {
    int version = this.client.checkExists().forPath(path).getVersion();
    try {
      this.client.delete().withVersion(version).forPath(path);
    } catch (Exception e) {
      Exception thrown = new Exception("Expected to delete version " + version + " + but ZK couldn\'t delete that version.", e);
      thrown.printStackTrace();
      throw thrown;
    }
    if (print) {
      report(this, "Deleted \"" + path + "\" with version=" + version);
    }
  }

  public static interface ManagedInstanceIdUser {
    public void idInvalidated(InstanceIdManager idm);

    public void newId(int id, InstanceIdManager idm);

    public void idManagementImpossible(InstanceIdManager idm);
  }

  @Override
  public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
    try {
      if (!this.ignoreWatches) {
        if (event.getPath().startsWith(this.instanceIdParentLockPath)) {
          /**
           * the lock code path rewatches on its own
           */
          synchronized (this.lock) {
            this.lock.notify();
          }
        } else if (event.getPath().equals(this.instanceIdParentPath)) {
          if (event.getType().equals(CuratorEventType.WATCHED)) {
            WatchedEvent we = event.getWatchedEvent();
            // System.out.println(we.getType());
            if (we.getType().equals(EventType.NodeDeleted)) {
              invalidate();
              // System.out.println(Integer.toHexString(this.hashCode()) + " " +
              // "Reregistering watch on " + this.instanceIdParentPath);
              getNewId();
              client.getData().watched().forPath(this.instanceIdParentPath);
            } else if (we.getType().equals(EventType.NodeChildrenChanged)) {
              // System.out.println(Integer.toHexString(this.hashCode()) + " " +
              // "Reregistering watch on children of " +
              // this.instanceIdParentPath);
              client.getChildren().watched().forPath(this.instanceIdParentPath);
              synchronized (this.fullLock) {
                // System.out.println(Integer.toHexString(this.hashCode()) + " "
                // +
                // "Notifying the locked instance waiting for full id space.");
                this.fullLock.notify();
              }
            } else {
              // System.out.println(Integer.toHexString(this.hashCode()) + " " +
              // "Reregistering watch on " + this.instanceIdParentPath);
              client.getData().watched().forPath(this.instanceIdParentPath);
            }
          }
        } else if (this.currentPath != null && this.currentPath.equals(event.getPath()) && event.getType().equals(CuratorEventType.WATCHED)) {
          WatchedEvent we = event.getWatchedEvent();
          if (we.getType().equals(EventType.NodeDeleted)) {
            byte[] data = this.client.getData().forPath(event.getPath());
            if (data == null) {
              // System.out.println(Integer.toHexString(this.hashCode()) + " " +
              // "Got NodeDeleted for " + event.getPath() +
              // " and it was actually missing.");
              invalidate();
              getNewId();
            } else if (!Arrays.equals(this.currentNodeUuid, data)) {
              System.out.println(Integer.toHexString(this.hashCode()) + " " + "Got NodeDeleted for " + event.getPath() + ". Its data is \""
                  + new String(data, "UTF-8") + "\".  My creation data was \"" + new String(this.currentNodeUuid, "UTF-8") + "\".");

              invalidate();
              getNewId();
            } else {
              System.out.println(Integer.toHexString(this.hashCode()) + " " + "Got NodeDeleted for " + event.getPath() + ". Its data is \""
                  + new String(data, "UTF-8") + "\".  My creation data was \"" + new String(this.currentNodeUuid, "UTF-8") + "\".  Not invalidating.");

            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      stop();
      this.iduser.idManagementImpossible(this);
    }
  }

  private void lock() throws Exception {
    // System.out.println(this + " " + "Attempting to lock...");
    makeLockParent();
    String lockPrefix = LOCK_NAME + "_";
    String lockPath = this.client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(this.instanceIdParentLockPath + "/" + lockPrefix);
    while (true) {
      synchronized (this.lock) {
        List<String> children = filterMatchesPrefix(this.client.getChildren().watched().forPath(this.instanceIdParentLockPath), lockPrefix);
        Collections.sort(children);
        String firstElement = children.iterator().next();
        if (lockPath.equals(this.instanceIdParentLockPath + "/" + firstElement)) {
          // got the lock
          this.currentLockPath = lockPath;
          return;
        } else {
          this.lock.wait(TIMEOUT);
        }
      }
    }
  }

  private void unlock() throws Exception {
    if (this.currentLockPath != null) {
      delete(this.currentLockPath, false);
      this.currentLockPath = null;
    }
  }

  private List<String> filterMatchesPrefix(List<String> source, String prefix) throws Exception {
    List<String> ret = new ArrayList<String>(source.size());
    for (String str : source) {
      if (str.startsWith(prefix)) {
        ret.add(str);
      }
    }

    return ret;
  }

  private void getNewId() throws Exception {
    makeParent();
    lock();
    try {
      int allocatedid = -1;
      while (allocatedid == -1) {
        List<String> idstrs = this.client.getChildren().forPath(instanceIdParentPath);
        if (idstrs.size() == this.maximumId) {
          switch (this.allocationPolicy) {
          case EXCEPTION:
            throw new InstanceIdSpaceFullException("Id space is full.  All " + this.maximumId + " instance id slots taken.");
          case BLOCKINDEFINITELY:
            /**
             * block indefinitely while we wait for a child node to be deleted
             */
            // System.out.println(Integer.toHexString(this.hashCode()) + " " +
            // "Id space is full.  Waiting for an event to notify us that we can take a look at the id space again.");
            synchronized (this.fullLock) {
              this.fullLock.wait();
            }
            // System.out.println(Integer.toHexString(this.hashCode()) + " " +
            // "Possibly no longer full, re-examining id space.");
            break;
          }
        } else {
          int foundunallocatedid = -1;
          if (idstrs.isEmpty()) {
            foundunallocatedid = 0;
          } else {
            int[] ids = new int[idstrs.size()];
            int counter = 0;
            for (String id : idstrs) {
              ids[counter] = Integer.parseInt(getLeafValueForNode(id), CHILD_NODE_NAME_RADIX);
              counter++;
            }
            Arrays.sort(ids);
            int expectedid = 0;
            for (int i = 0; i < ids.length && foundunallocatedid == -1; i++) {
              if (ids[i] != expectedid) {
                foundunallocatedid = expectedid;
              }
              expectedid++;
            }
            // System.out.println("Found id while looping through: " +
            // foundunallocatedid);
            if (foundunallocatedid == -1 && ids.length < maximumId) {
              foundunallocatedid = expectedid++;
              // System.out.println("Found id after after looping through: " +
              // foundunallocatedid);
            }
          }
          if (foundunallocatedid == -1) {
            throw new Exception("Could not allocate an instance id.  The maximum number of instances are operating.");
          }

          String path = this.instanceIdParentPath + PATH_SEPARATOR_CHAR + Integer.toString(foundunallocatedid, CHILD_NODE_NAME_RADIX);
          UUID uuid = UUID.randomUUID();
          this.currentNodeUuid = uuid.toString().getBytes("UTF-8");
          this.currentPath = this.client.create().withMode(CreateMode.EPHEMERAL).forPath(path, this.currentNodeUuid);
          this.client.getData().watched().forPath(this.currentPath);
          allocatedid = foundunallocatedid;
          this.currentId = allocatedid;
          this.iduser.newId(this.currentId, this);
        }
      }
    } finally {
      unlock();
    }
  }

  private static final char PATH_SEPARATOR_CHAR = '/';

  private static String getLeafValueForNode(String path) {
    int lastIndex = path.lastIndexOf(PATH_SEPARATOR_CHAR);
    return lastIndex >= 0 ? path.substring(lastIndex + 1) : path;
  }
}
