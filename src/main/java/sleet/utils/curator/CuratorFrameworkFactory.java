package sleet.utils.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Basic factory for creating CuratorFramework instances to be used to
 * coordinate with other sleet id generators in the same ZK cluster.
 * 
 * @author mmead
 * 
 */

public class CuratorFrameworkFactory {

  /**
   * Generates a CuratorFramework instance for the specific ZK quorum, using the
   * ExponentialBackoffRetry strategy with specified base sleep time and number
   * of retries. Starts the instance before returning it. If this is intended to
   * be a singleton, the caller should guard it appropriately.
   * 
   * @param zkQuorum
   *          the ZK quorum string (including setroot if desired)
   * @param baseSleepTimeMs
   *          base sleep time for the exponential backoff retry policy for ZK
   *          interaction
   * @param maxRetries
   *          maximum number of times to retry operations
   * @return a started CuratorFramework instance
   */
  public static CuratorFramework instance(String zkQuorum, int baseSleepTimeMs, int maxRetries) {
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
    CuratorFramework curatorClient = org.apache.curator.framework.CuratorFrameworkFactory.newClient(zkQuorum, retryPolicy);
    curatorClient.start();

    return curatorClient;
  }

}
