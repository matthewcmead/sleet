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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sleet.generators.time.TimeDependentSequenceIdGenerator;
import sleet.utils.zookeeper.ZkMiniCluster;

public class TestSleetIdGenerator_UnitTest {
  public static final int NUM_INSTANCES = 100;

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

  @Test
  public void testSleetIdGeneratorNonReentrant() throws IOException, InterruptedException {
    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
    Thread[] threads = new Thread[NUM_INSTANCES];
    final Object lock = new Object();
    final AtomicReference<Exception> exception = new AtomicReference<Exception>();
    for (int instance = 0; instance < NUM_INSTANCES; instance++) {
      Thread t = new Thread() {

        @Override
        public void run() {
          try {
            SleetIdGenerator gen = new SleetIdGenerator();
            Properties props = new Properties();
            InputStream is = gen.getClass().getResourceAsStream("sleet.properties");
            props.load(is);
            is.close();
            long beforeSession = System.currentTimeMillis();
            gen.beginIdSession(props);
            long startupTime = System.currentTimeMillis() - beforeSession;
            System.out.println("Session startup time: " + startupTime + "ms.");
            int loops = 100000;
            long start = System.currentTimeMillis();
            long maxseq = (1L << Integer.parseInt(props.getProperty(TimeDependentSequenceIdGenerator.BITS_IN_SEQUENCE_KEY))) - 1L;
            long highest = -1;
            for (int i = 0; i < loops; i++) {
              // System.out.println(SleetIdGenerator.paddedBinary(gen.getId().getId()));
              long id = gen.getId().getId();
              long seq = id & maxseq;
              if (seq > highest) {
                highest = seq;
              }
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

  @Test
  public void testSleetIdGeneratorReentrant() throws IOException, InterruptedException, SleetException {
    Thread[] threads = new Thread[NUM_INSTANCES];
    final Object lock = new Object();
    final AtomicReference<Exception> exception = new AtomicReference<Exception>();
    final SleetIdGenerator gen = new SleetIdGenerator();
    final Properties props = new Properties();
    InputStream is = gen.getClass().getResourceAsStream("sleet.properties");
    props.load(is);
    is.close();
    long beforeSession = System.currentTimeMillis();
    gen.beginIdSession(props);
    long startupTime = System.currentTimeMillis() - beforeSession;
    System.out.println("Session startup time: " + startupTime + "ms.");
    final int loops = 100000;
    final HashMap<Long, Boolean> ids = new HashMap<Long, Boolean>(loops * NUM_INSTANCES);
    final Object idlock = new Object();
    for (int instance = 0; instance < NUM_INSTANCES; instance++) {
      Thread t = new Thread() {

        @Override
        public void run() {
          try {
            long start = System.currentTimeMillis();
            long maxseq = (1L << Integer.parseInt(props.getProperty(TimeDependentSequenceIdGenerator.BITS_IN_SEQUENCE_KEY))) - 1L;
            long highest = -1;
            for (int i = 0; i < loops; i++) {
              // System.out.println(SleetIdGenerator.paddedBinary(gen.getId().getId()));
              long id = gen.getId().getId();
              synchronized (idlock) {
                if (ids.containsKey(id)) {
                  String errmsg = "Collided with another thread on id: " + id;
                  System.out.println(errmsg);
                  exception.set(new Exception(errmsg));
                }
                ids.put(id, Boolean.TRUE);
              }
              long seq = id & maxseq;
              if (seq > highest) {
                highest = seq;
              }
            }
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
    gen.endIdSession();
  }

}
