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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import sleet.generators.GeneratorConfigException;
import sleet.generators.GeneratorException;
import sleet.generators.GeneratorSessionException;
import sleet.generators.IdGenerator;
import sleet.generators.IdOverflowException;
import sleet.generators.IdOverrunException;
import sleet.generators.fixed.FixedLongIdGenerator;
import sleet.generators.reservedinstance.ZooKeeperReservedInstanceIdGenerator;
import sleet.generators.time.SequenceIdOverflowError;
import sleet.generators.time.TimeDependentSequenceIdGenerator;
import sleet.generators.time.TimeIdGenerator;
import sleet.generators.time.TimeIdReverseSkewError;
import sleet.id.IdType;
import sleet.id.LongId;
import sleet.id.LongIdType;
import sleet.id.TimeIdType;
import sleet.state.IdState;

public class SleetIdGenerator implements IdGenerator<LongIdType> {
  public static final String WAIT_ON_SEQUENCE_OVERRUN_KEY = "sleet.wait.on.sequence.overrun";

  private final List<IdGenerator<? extends IdType<?, ?>>> subgens = new ArrayList<IdGenerator<? extends IdType<?, ?>>>(4);

  private boolean waitOnSeqOverrun;

  private Object lock = new Object();

  public SleetIdGenerator() {
  }

  @Override
  public void beginIdSession(Properties config) throws SleetException {
    if (!this.subgens.isEmpty()) {
      throw new GeneratorSessionException("Session was already started.  Stop session by calling endIdSession() then start session by calling beginIdSession()");
    }
    TimeIdGenerator timeGen = new TimeIdGenerator();
    timeGen.beginIdSession(config);
    this.subgens.add(timeGen);

    FixedLongIdGenerator fixedGen = new FixedLongIdGenerator();
    fixedGen.beginIdSession(config);
    this.subgens.add(fixedGen);

    ZooKeeperReservedInstanceIdGenerator instanceGen = new ZooKeeperReservedInstanceIdGenerator();
    instanceGen.beginIdSession(config);
    this.subgens.add(instanceGen);

    TimeDependentSequenceIdGenerator seqGen = new TimeDependentSequenceIdGenerator();
    seqGen.beginIdSession(config);
    this.subgens.add(seqGen);

    String waitOnSeqOverrunStr = config.getProperty(WAIT_ON_SEQUENCE_OVERRUN_KEY);
    if (waitOnSeqOverrunStr == null) {
      throw new GeneratorConfigException(
          "Missing flag for whether to wait on sequence overrun during id generation, must be specified in configuration properties key \""
              + WAIT_ON_SEQUENCE_OVERRUN_KEY + "\".");
    }
    this.waitOnSeqOverrun = Boolean.parseBoolean(waitOnSeqOverrunStr);
  }

  @Override
  public void checkSessionValidity() throws SleetException {
    validateSessionStarted();
    for (IdGenerator<?> subgen : this.subgens) {
      subgen.checkSessionValidity();
    }
  }

  private void validateSessionStarted() throws GeneratorSessionException {
    if (this.subgens.isEmpty()) {
      throw new GeneratorSessionException("Session was not started.  Start session by calling beginIdSession()");
    }
  }

  @Override
  public void endIdSession() throws SleetException {
    validateSessionStarted();
    for (IdGenerator<?> subgen : this.subgens) {
      subgen.endIdSession();
    }
    this.subgens.clear();
  }

  public LongIdType getId() throws SleetException {
    return getId(null);
  }

  @Override
  public LongIdType getId(List<IdState<?, ?>> states) throws SleetException {
    validateSessionStarted();
    synchronized (lock) {
      List<IdState<?, ?>> substates = new ArrayList<IdState<?, ?>>(4);
      long numBits = 0;
      for (IdGenerator<? extends IdType<?, ?>> subgen : this.subgens) {
        IdType<?, ?> subid = subgen.getId(substates);
        if (subid.getError() != null) {
          if (subid.getError() instanceof SequenceIdOverflowError) {
            if (waitOnSeqOverrun) {
              Long lastTimeValue = null;
              for (int i = 0; i < substates.size() && lastTimeValue == null; i++) {
                IdState<?, ?> substate = substates.get(i);
                if (substate.getId() instanceof TimeIdType) {
                  lastTimeValue = ((TimeIdType) substate.getId()).getId();
                }
              }
              if (lastTimeValue == null) {
                throw new GeneratorException("Time dependent sequence overflowed, but unable to find time id in existing id states upon which to base a sleep.");
              }
              /**
               * Sleep until next time value and re-run the id generation
               * process
               */
              ((TimeIdGenerator) this.subgens.get(0)).sleepUntilNextTimeValue(lastTimeValue);
              return getId(states);
            } else {
              throw new IdOverrunException("Time dependent sequence overflowed and Sleet is currently configured not to wait until new time value to proceed.");
            }
          } else if (subid.getError() instanceof TimeIdReverseSkewError) {
            throw new GeneratorException("Time id component encountered reverse time skew and did not properly handle it. Messages was: "
                + subid.getError().getErrorMessage());
          } else {
            throw new GeneratorException("Unknown IdError encountered: " + subid.getError().getClass().getName() + "; Its message: "
                + subid.getError().getErrorMessage());
          }
        } else {
          @SuppressWarnings({ "unchecked", "rawtypes" })
          IdState<?, ?> newstate = new IdState(subgen.getClass(), subid);
          substates.add(newstate);
          numBits += subid.getNumBitsInId();
        }
      }
      if (numBits > 64) {
        throw new IdOverflowException("Sleet is prepared to use 64 bits for id output, but the underlying component id generators produced a total of "
            + numBits + " bits of id data.");
      }

      long output = 0;
      int rotated = 0;
      for (IdState<?, ?> substate : substates) {
        IdType<?, ?> subid = substate.getId();
        int idbits = subid.getNumBitsInId();
        Object idO = subid.getId();
        if (idO instanceof Long) {
          long id = (Long) idO;
          long idmask = (1L << idbits) - 1L;
          // System.out.println(paddedBinary(id & idmask));
          // System.out.println("rotating left " + (64 - rotated - idbits) +
          // " bits.");
          output |= (Long.rotateLeft(idmask, 64 - rotated - idbits) & Long.rotateLeft(id, 64 - rotated - idbits));
          rotated += idbits;
          // System.out.println(paddedBinary(output));
        } else {
          throw new GeneratorException("Expected an id type of Long but got " + idO.getClass().getName());
        }
      }

      return new LongId(output, null, 64);
    }
  }

  static String paddedBinary(long value) {
    return String.format("%64s", Long.toBinaryString(value)).replace(' ', '0');
  }
}
