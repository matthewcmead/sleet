package sleet.generators.reservedinstance;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import sleet.SleetException;
import sleet.generators.GeneratorConfigException;
import sleet.generators.GeneratorSessionException;
import sleet.generators.IdGenerator;
import sleet.id.LongId;
import sleet.id.LongIdType;
import sleet.state.IdState;
import sleet.utils.zookeeper.InstanceIdManagerZooKeeper;

public class ZooKeeperReservedInstanceIdGenerator implements IdGenerator<LongIdType> {

  public static final String ZK_SERVER_KEY = "zk.reserved.instance.zk.server";
  public static final String ZK_PATH_KEY = "zk.reserved.instance.zk.path";
  public static final String ZK_TIMEOUT_KEY = "zk.reserved.instance.zk.timeout";
  public static final String BITS_IN_INSTANCE_ID_KEY = "zk.reserved.instance.bits.in.instance.value";
  public static final String MILLISECONDS_TO_WAIT_FOR_ID = "zk.reserved.instance.milliseconds.to.wait.for.startup";

  private int bitsInInstanceValue = -1;
  private int maxInstanceValue = -1;
  private ZooKeeper zk = null;
  private String path = null;
  private InstanceIdManagerZooKeeper instMgr = null;
  private LongId id = null;

  public ZooKeeperReservedInstanceIdGenerator() {
  }

  @Override
  public void beginIdSession(Properties config) throws SleetException {
    if (this.zk != null) {
      throw new GeneratorSessionException("Session was already started.  Stop session by calling endIdSession() then start session by calling beginIdSession()");
    }

    String zkStr = config.getProperty(ZK_SERVER_KEY);
    if (zkStr == null) {
      throw new GeneratorConfigException("Missing ZooKeeper server string, must be specified in configuration properties key \"" + ZK_SERVER_KEY + "\".");
    }

    String zkPathStr = config.getProperty(ZK_PATH_KEY);
    if (zkPathStr == null) {
      throw new GeneratorConfigException("Missing ZooKeeper path string, must be specified in configuration properties key \"" + ZK_PATH_KEY + "\".");
    }

    this.path = zkPathStr;

    String zkTimeoutStr = config.getProperty(ZK_TIMEOUT_KEY);
    if (zkTimeoutStr == null) {
      throw new GeneratorConfigException("Missing ZooKeeper timeout, must be specified in configuration properties key \"" + ZK_TIMEOUT_KEY + "\".");
    }
    int zkTimeout = -1;
    try {
      zkTimeout = Integer.valueOf(zkTimeoutStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse ZooKeeper timeout from value \"" + zkTimeoutStr + "\".  The value must be an integer.");
    }

    try {
      this.zk = new ZooKeeper(zkStr, zkTimeout, new Watcher() {
        @Override
        public void process(WatchedEvent event) {

        }
      });
    } catch (IOException e) {
      throw new SleetException(e);
    }

    String bitsStr = config.getProperty(BITS_IN_INSTANCE_ID_KEY);
    if (bitsStr == null) {
      throw new GeneratorConfigException("Missing number of bits, must be specified in configuration properties key \"" + BITS_IN_INSTANCE_ID_KEY + "\".");
    }
    int bits = -1;
    try {
      bits = Integer.valueOf(bitsStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse number of bits from value \"" + bitsStr + "\".  The value must be an integer.");
    }
    this.bitsInInstanceValue = bits;
    this.maxInstanceValue = (1 << this.bitsInInstanceValue) - 1;

    String msTimeoutStr = config.getProperty(MILLISECONDS_TO_WAIT_FOR_ID);
    if (msTimeoutStr == null) {
      throw new GeneratorConfigException("Missing number of milliseconds to wait on startup, must be specified in configuration properties key \"" + MILLISECONDS_TO_WAIT_FOR_ID + "\".");
    }
    int msTimeout = -1;
    try {
      msTimeout = Integer.valueOf(msTimeoutStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse number of milliseconds to wait on startup from value \"" + msTimeoutStr + "\".  The value must be an integer.");
    }

    try {
      this.instMgr = new InstanceIdManagerZooKeeper(this.zk, this.path, this.maxInstanceValue + 1);
      long underlyingid = this.instMgr.tryToGetId(msTimeout);
      if (underlyingid == -1) {
        throw new ReservedInstanceTimeoutException("Unable to allocate an id instance within the timeout allotted (" + msTimeout + ")");
      } else {
        this.id = new LongId(underlyingid, null, this.bitsInInstanceValue);
      }
    } catch (IOException e) {
      throw new SleetException(e);
    }
  }

  @Override
  public void checkSessionValidity() throws SleetException {
    validateSessionStarted();
    if (!this.instMgr.sessionValid()) {
      throw new GeneratorSessionException("Underlying " + this.instMgr.getClass().getName() + " implementation lost session validity.");
    }
  }

  @Override
  public void endIdSession() throws SleetException {
    validateSessionStarted();
    try {
      this.instMgr.releaseId(this.id.getId().intValue());
    } catch (IOException e) {
      throw new SleetException(e);
    }
    try {
      this.zk.close();
    } catch (InterruptedException e) {
      throw new SleetException(e);
    }
    this.bitsInInstanceValue = -1;
    this.maxInstanceValue = -1;
    this.zk = null;
    this.path = null;
    this.instMgr = null;
    this.id = null;
  }

  @Override
  public LongIdType getId(List<IdState<?, ?>> states) throws SleetException {
    validateSessionStarted();
    return this.id;
  }

  private void validateSessionStarted() throws GeneratorSessionException {
    if (this.id == null) {
      throw new GeneratorSessionException("Session was not started.  Start session by calling beginIdSession()");
    }
  }

}
