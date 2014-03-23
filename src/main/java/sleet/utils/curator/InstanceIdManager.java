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

public class InstanceIdManager implements CuratorListener {
  private static int CHILD_NODE_NAME_RADIX = 16;
  private CuratorFramework client = null;
  private String instanceIdParentPath = null;
  private ManagedInstanceIdUser iduser = null;
  private int maximumId;
  private int currentId;

  public InstanceIdManager(String instanceIdParentPath, CuratorFramework client, ManagedInstanceIdUser iduser, int maximumId) {
    this.instanceIdParentPath = instanceIdParentPath;
    this.client = client;
    this.iduser = iduser;
    this.maximumId = maximumId;
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

  public static interface ManagedInstanceIdUser {
    public void idInvalidated();

    public void newId(int id);

    public void idManagementImpossible();
  }

  @Override
  public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
    System.out.println("eventReceived: " + event.getPath() + "; " + event.getType());
    if (instanceIdParentPath.equals(event.getPath()) && event.getType().equals(CuratorEventType.WATCHED)) {
      WatchedEvent we = event.getWatchedEvent();
      if (we.getType().equals(EventType.NodeDeleted)) {
        this.iduser.idInvalidated();
        getNewId();
      }
      client.getData().watched().forPath(this.instanceIdParentPath);
    }
    /**
     * TODO MCM also detect deletion of id node and appropriately get new id
     */
  }

  private void getNewId() throws Exception {
    makeParent();
    int allocatedid = -1;
    while (allocatedid == -1) {
      List<String> idstrs = this.client.getChildren().forPath(instanceIdParentPath);
      int foundunallocatedid = -1;
      if (idstrs.isEmpty()) {
        foundunallocatedid = 0;
      } else {
        /**
         * TODO MCM when the list is as long as the maximum number of ids we can
         * allocate, we either need to fail or wait
         */
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
        client.create().withMode(CreateMode.EPHEMERAL).forPath(this.instanceIdParentPath + PATH_SEPARATOR_CHAR + Integer.toString(foundunallocatedid, CHILD_NODE_NAME_RADIX));
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

    }, 4096);
    idm.start();
    while (true) {
      Thread.sleep(1000);
    }
  }

}
