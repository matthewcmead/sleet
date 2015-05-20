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
package sleet;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sleet.generators.IdGenerator;
import sleet.generators.reservedinstance.ZooKeeperReservedInstanceIdGenerator;
import sleet.generators.reservedinstance.ZooKeeperReservedInstanceIdGeneratorProxy;
import sleet.generators.time.TimeDependentSequenceIdGenerator;
import sleet.id.IdType;
import sleet.utils.zookeeper.ZkMiniCluster;

public class TestSleetIdGeneratorReaping_UnitTest {
  public static final int NUM_WELL_BEHAVED_INSTANCES = 16;
  public static final int NUM_ABORTING_INSTANCES = 4;

  private ZkMiniCluster cluster = null;
  private String zkconn = null;

  @BeforeClass
  public static void setupClass() {
    ConsoleAppender console = new ConsoleAppender(); // create appender
    // configure the appender
    String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.FATAL);
    console.activateOptions();
    // add appender to any Logger (here is root)
    Logger.getRootLogger().addAppender(console);
    console = new ConsoleAppender(); // create appender
    // configure the appender
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.INFO);
    console.activateOptions();
    // add appender to any Logger (here is root)
    Logger.getLogger("sleet").addAppender(console);
    console = new ConsoleAppender(); // create appender
    // configure the appender
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.DEBUG);
    console.activateOptions();
    // add appender to any Logger (here is root)
    // Logger.getLogger("sleet.utils.zookeeper.InstanceIdManagerForwardReservationZooKeeper").addAppender(console);
  }

  @Before
  public void setup() throws IOException {
    cluster = new ZkMiniCluster();
    File temp = File.createTempFile("instanceidmanager", "");
    temp.delete();
    temp.mkdir();
    System.out.println("zk temp path: " + temp.getAbsolutePath());
    cluster.startZooKeeper(temp.getAbsolutePath());
    zkconn = cluster.getZkConnectionString();
    zkconn = "localhost:" + zkconn.substring(zkconn.lastIndexOf(':') + 1);
    System.out.println("ZK connection string: " + zkconn);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdownZooKeeper();
    }
  }

  private static void setupProperties(Properties props) {
    props.setProperty("sleet.wait.on.sequence.overrun", "true");
    props.setProperty("time.epoch.in.java.system.time.millis", "1394827063000");
    props.setProperty("time.granularity.milliseconds", "1");
    props.setProperty("time.bits.in.time.value", "41");
    props.setProperty("time.max.wait.milliseconds.after.backward.clock.skew", "10000");
    props.setProperty("fixed.long.value", "0");
    props.setProperty("fixed.bits.in.value", "11");
    props.setProperty("zk.reserved.instance.zk.server", "localhost:21810");
    props.setProperty("zk.reserved.instance.zk.path", "/sleet_datacenter_1");
    props.setProperty("zk.reserved.instance.zk.timeout", "40000");
    props.setProperty("zk.reserved.instance.bits.in.instance.value", "4");
    props.setProperty("zk.reserved.instance.milliseconds.to.wait.for.startup", "15000");
    props.setProperty("sequence.bits.in.sequence.value", "8");
    props.setProperty("zk.instmgr.fwdreservation.reservation.check.period.ms", "500");
    props.setProperty("zk.instmgr.fwdreservation.must.lock.threshold.fraction", "0.99");
    props.setProperty("zk.instmgr.fwdreservation.stop.lock.threshold.fraction", "0.90");
    props.setProperty("zk.instmgr.fwdreservation.max.zk.clock.offset.ms", "100");
    props.setProperty("zk.instmgr.fwdreservation.reservation.length.ms", "3000");
    props.setProperty("zk.instmgr.fwdreservation.reservation.reap.graceperiod.ms", "8000");
  }

  @Test
  public void testSleetIdGeneratorNonReentrant() throws IOException, InterruptedException {
    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
    Thread[] threads = new Thread[NUM_ABORTING_INSTANCES + NUM_WELL_BEHAVED_INSTANCES];
    final Object lock = new Object();
    final AtomicReference<Exception> exception = new AtomicReference<Exception>();
    for (int instance = 0; instance < NUM_ABORTING_INSTANCES; instance++) {
      Thread t = new Thread() {

        @Override
        public void run() {
          try {
            SleetIdGenerator gen = new SleetIdGenerator();
            Properties props = new Properties();
            setupProperties(props);
            gen.beginIdSession(props);
            ZooKeeperReservedInstanceIdGenerator zkGen = getZkGen(gen);
            assertNotNull(zkGen);
            ZooKeeper zk = ZooKeeperReservedInstanceIdGeneratorProxy.getZk(zkGen);
            assertNotNull(zk);
            zk.close();
            synchronized (lock) {
              lock.notify();
            }
          } catch (Exception e) {
            e.printStackTrace();
            exception.set(e);
            return;
          }
        }

      };
      threads[instance] = t;
      t.start();
    }
    for (int instance = NUM_ABORTING_INSTANCES; instance < threads.length; instance++) {
      Thread t = new Thread() {

        @Override
        public void run() {
          try {
            SleetIdGenerator gen = new SleetIdGenerator();
            Properties props = new Properties();
            setupProperties(props);
            long beforeSession = System.currentTimeMillis();
            gen.beginIdSession(props);
            long startupTime = System.currentTimeMillis() - beforeSession;
            System.out.println("Session startup time: " + startupTime + "ms.");
            int loops = 0;
            long start = System.currentTimeMillis();
            long maxseq = (1L << Integer.parseInt(props.getProperty(TimeDependentSequenceIdGenerator.BITS_IN_SEQUENCE_KEY))) - 1L;
            long highest = -1;
            while (System.currentTimeMillis() - start < 16000) {
              // System.out.println(SleetIdGenerator.paddedBinary(gen.getId().getId()));
              long id = gen.getId().getId();
              long seq = id & maxseq;
              if (seq > highest) {
                highest = seq;
              }
              loops++;
            }
            gen.endIdSession();
            long duration = System.currentTimeMillis() - start;
            System.out.println("Duration for " + loops + " ids: " + duration + "ms");
            System.out.println("Mean ids per ms: " + loops / duration);
            System.out.println("Max sequence: " + highest);
            synchronized (lock) {
              lock.notify();
            }
          } catch (Exception e) {
            e.printStackTrace();
            exception.set(e);
            return;
          }
        }

      };
      threads[instance] = t;
      t.start();
    }
    boolean finished = false;
    while (!finished) {
      synchronized (lock) {
        lock.wait(TimeUnit.SECONDS.toMillis(1));
      }
      boolean alive = false;
      for (int instance = 0; instance < threads.length && !alive; instance++) {
        if (threads[instance].isAlive()) {
          alive = true;
        }
      }
      finished = !alive;
      if (exception.get() != null) {
        exception.get().printStackTrace();
      }
      Assert.assertNull(exception.get());
    }
    if (exception.get() != null) {
      exception.get().printStackTrace();
    }
    Assert.assertNull(exception.get());
  }

  private static ZooKeeperReservedInstanceIdGenerator getZkGen(SleetIdGenerator sleetidgen) {
    for (IdGenerator<? extends IdType<?, ?>> subgen : sleetidgen.subgens) {
      if (subgen instanceof ZooKeeperReservedInstanceIdGenerator) {
        return (ZooKeeperReservedInstanceIdGenerator) subgen;
      }
    }
    return null;
  }
}
