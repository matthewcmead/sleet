package sleet.utils.curator;

import java.util.Arrays;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

//TODO MCM update this based on discussion with Aaron regarding the need to minimize "racing" against zookeeper.
//TODO MCM come up with an exception model for this class instead of throwing base level exceptions.

/**
 * A class to manage (over zookeeper) the act of allocating a currently unique
 * instance id within a single zookeeper quorum.
 * 
 * @author mmead
 * 
 */

public class InstanceIdManager implements CuratorListener {
  private static final int CHILD_NODE_NAME_RADIX = 16;
  private CuratorFramework client = null;
  private String instanceIdParentPath = null;
  private ManagedInstanceIdUser iduser = null;
  private final int maximumId;
  private int currentId;
  private String currentPath = null;
  private final IdContentionPolicy allocationPolicy;
  private boolean ignoreWatches = false;

  public static enum IdContentionPolicy {
    BLOCKINDEFINITELY, EXCEPTION
  }

  public InstanceIdManager(String instanceIdParentPath, CuratorFramework client, ManagedInstanceIdUser iduser, int maximumId, IdContentionPolicy allocationPolicy) {
    this.instanceIdParentPath = instanceIdParentPath;
    this.client = client;
    this.iduser = iduser;
    this.maximumId = maximumId;
    this.allocationPolicy = allocationPolicy;
  }

  private void makeParent() throws Exception {
    try {
      client.create().creatingParentsIfNeeded().forPath(this.instanceIdParentPath);
    } catch (NodeExistsException e) {
      // ignore
    }
  }

  public void start() throws Exception {
    makeParent();
    client.getCuratorListenable().addListener(this);
    client.getData().watched().forPath(this.instanceIdParentPath);
    getNewId();
  }

  public void stop() throws Exception {
    if (this.currentPath != null) {
      this.ignoreWatches = true;
      client.delete().forPath(this.currentPath);
    }
  }

  public static interface ManagedInstanceIdUser {
    public void idInvalidated();

    public void newId(int id);

    public void idManagementImpossible();
  }

  @Override
  public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
    System.out.println("eventReceived: " + event.getPath() + "; " + event.getType());
    if (!this.ignoreWatches) {
      if (this.instanceIdParentPath.equals(event.getPath()) && event.getType().equals(CuratorEventType.WATCHED)) {
        WatchedEvent we = event.getWatchedEvent();
        if (we.getType().equals(EventType.NodeDeleted)) {
          this.iduser.idInvalidated();
          getNewId();
        }
        client.getData().watched().forPath(this.instanceIdParentPath);
      } else if (this.currentPath != null && this.currentPath.equals(event.getPath()) && event.getType().equals(CuratorEventType.WATCHED)) {
        WatchedEvent we = event.getWatchedEvent();
        if (we.getType().equals(EventType.NodeDeleted)) {
          this.iduser.idInvalidated();
          getNewId();
        }
      }
    }
  }

  private void getNewId() throws Exception {
    makeParent();
    int allocatedid = -1;
    while (allocatedid == -1) {
      List<String> idstrs = this.client.getChildren().forPath(instanceIdParentPath);
      if (idstrs.size() == this.maximumId) {
        switch (this.allocationPolicy) {
        case EXCEPTION:
          /**
           * TODO MCM use special exception type here for detection by calling
           * class
           */
          throw new Exception("Id space full.");
        case BLOCKINDEFINITELY:
          /**
           * TODO MCM properly block indefinitely while we wait for a child node
           * to be deleted
           */
          break;
        }
      }
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
        if (foundunallocatedid == -1 && ids.length < maximumId) {
          foundunallocatedid = expectedid++;
        }
      }
      if (foundunallocatedid == -1) {
        throw new Exception("Could not allocate an instance id.  The maximum number of instances are operating.");
      }

      try {
        String path = this.instanceIdParentPath + PATH_SEPARATOR_CHAR + Integer.toString(foundunallocatedid, CHILD_NODE_NAME_RADIX);
        client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        this.currentPath = path;
        client.getData().watched().forPath(path);
        allocatedid = foundunallocatedid;
      } catch (NodeExistsException e) {
        // ignore
      }
    }
    this.currentId = allocatedid;
    this.iduser.newId(this.currentId);
  }

  private static final char PATH_SEPARATOR_CHAR = '/';

  private static String getLeafValueForNode(String path) {
    int lastIndex = path.lastIndexOf(PATH_SEPARATOR_CHAR);
    return lastIndex >= 0 ? path.substring(lastIndex + 1) : path;
  }

  public static void main(String[] args) throws Exception {
    CuratorFramework client = CuratorFrameworkFactory.instance("mini4,mini7,mini8/sleet", 1000, 1);
    InstanceIdManager idm = new InstanceIdManager("/sleet/datacenter/1", client, new ManagedInstanceIdUser() {

      @Override
      public void idInvalidated() {
        System.out.println("We lost our id!");
      }

      @Override
      public void newId(int id) {
        System.out.println("We got a new id of: " + id);
      }

      @Override
      public void idManagementImpossible() {
        System.out.println("Id management is no longer possible!");
      }

    }, 4096, IdContentionPolicy.BLOCKINDEFINITELY);
    idm.start();
    while (true) {
      Thread.sleep(1000);
    }
  }

}
