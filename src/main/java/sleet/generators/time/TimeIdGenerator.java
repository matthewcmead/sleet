package sleet.generators.time;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import sleet.SleetException;
import sleet.generators.GeneratorConfigException;
import sleet.generators.GeneratorSessionException;
import sleet.generators.IdGenerator;
import sleet.id.TimeId;
import sleet.id.TimeIdType;
import sleet.state.IdState;
import sleet.utils.time.TimeCalculationException;
import sleet.utils.time.TimeCalculator;

/**
 * 
 * 
 * @author mmead
 * 
 */
public class TimeIdGenerator implements IdGenerator<TimeIdType> {

  public static final String EPOCH_KEY = "time.epoch.in.java.system.time.millis";
  public static final String GRANULARITY_IN_MS_KEY = "time.granularity.milliseconds";
  public static final String BITS_IN_TIME_VALUE_KEY = "time.bits.in.time.value";
  public static final String MAX_WAIT_AFTER_BACKWARD_CLOCK_SKEW_KEY = "time.max.wait.ms.after.backward.clock.skew";

  private TimeCalculator timeCalc;
  private long maxWaitAfterBackwardClockSkew = -1;
  private long lastTimeValue = -1;

  @Override
  public void endIdSession() throws SleetException {
    validateSessionStarted();
    this.timeCalc = null;
  }

  @Override
  public void beginIdSession(Properties config) throws SleetException {
    if (this.timeCalc != null) {
      throw new GeneratorSessionException("Session was already started.  Stop session by calling endIdSession() then start session by calling beginIdSession()");
    }
    String epochStr = config.getProperty(EPOCH_KEY);
    if (epochStr == null) {
      throw new GeneratorConfigException("Missing epoch for time calculation, must be specified in configuration properties key \"" + EPOCH_KEY + "\".");
    }
    long epoch = -1;
    try {
      epoch = Long.valueOf(epochStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse epoch time from value \"" + epochStr + "\".  The value for configuration properties key \"" + EPOCH_KEY + "\" must be a long.");
    }
    if (epoch < 0) {
      throw new GeneratorConfigException("Epoch specified (" + epoch + ") is negative.");
    }
    if (epoch > System.currentTimeMillis()) {
      throw new GeneratorConfigException("Epoch specified (" + epoch + ") is in the future.");
    }
    String granularityStr = config.getProperty(GRANULARITY_IN_MS_KEY);
    if (granularityStr == null) {
      throw new GeneratorConfigException("Missing granularity (in milliseconds), must be specified in configuration properties key \"" + GRANULARITY_IN_MS_KEY + "\".");
    }
    long granularityMs = -1;
    try {
      granularityMs = Long.valueOf(granularityStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse granularity (in milliseconds) from value \"" + granularityStr + "\".  The value for configuration properties key \"" + GRANULARITY_IN_MS_KEY
          + "\" must be a long.");
    }
    if (granularityMs < 1) {
      throw new GeneratorConfigException("Granularity < 1ms is not supported.");
    }
    String bitsStr = config.getProperty(BITS_IN_TIME_VALUE_KEY);
    if (bitsStr == null) {
      throw new GeneratorConfigException("Missing number of bits, must be specified in configuration properties key \"" + BITS_IN_TIME_VALUE_KEY + "\".");
    }
    int bits = -1;
    try {
      bits = Integer.valueOf(bitsStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse number of bits from value \"" + bitsStr + "\".  The value must be an integer.");
    }

    this.timeCalc = new TimeCalculator(epoch, granularityMs, TimeUnit.MILLISECONDS, bits);

    String maxWaitAfterBackwardClockSkewStr = config.getProperty(MAX_WAIT_AFTER_BACKWARD_CLOCK_SKEW_KEY);
    if (maxWaitAfterBackwardClockSkewStr == null) {
      throw new GeneratorConfigException("Missing maximum wait (in ms) after backward time skew, must be specified in configuration properties key \"" + MAX_WAIT_AFTER_BACKWARD_CLOCK_SKEW_KEY + "\".");
    }
    long maxWaitAfterBackwardClockSkew = -1;
    try {
      maxWaitAfterBackwardClockSkew = Long.valueOf(maxWaitAfterBackwardClockSkewStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse maximum wait (in ms) after backward time skew from value \"" + maxWaitAfterBackwardClockSkewStr + "\".  The value must be a long.");
    }

    this.maxWaitAfterBackwardClockSkew = maxWaitAfterBackwardClockSkew;
  }

  @Override
  public TimeIdType getId(List<IdState<?, ?>> states) throws SleetException {
    validateSessionStarted();
    /**
     * MCM validate the clock didn't move backward enough to reduce the time
     * value since last time
     */
    long timeValue = this.timeCalc.timeValue();
    if (timeValue < this.lastTimeValue) {
      long deltaMs = this.timeCalc.millisSinceJavaEpochUTC(this.lastTimeValue) - this.timeCalc.millisSinceJavaEpochUTC(timeValue);
      if (deltaMs <= this.maxWaitAfterBackwardClockSkew) {
        /**
         * MCM if config allows us to sleep, just sleep, otherwise we error
         * below for the process above to handle
         */
        while (timeValue < this.lastTimeValue) {
          try {
            Thread.sleep(deltaMs);
          } catch (InterruptedException e) {
            // ignore
          }
          timeValue = this.timeCalc.timeValue();
          deltaMs = this.timeCalc.millisSinceJavaEpochUTC(this.lastTimeValue) - this.timeCalc.millisSinceJavaEpochUTC(timeValue);
        }
        this.lastTimeValue = timeValue;
        return new TimeId(timeValue, null);
      } else {
        return new TimeId(timeValue, new TimeIdReverseSkewError("Time skewed backward by " + deltaMs + "ms.", deltaMs));
      }
    } else {
      this.lastTimeValue = timeValue;
      return new TimeId(timeValue, null);
    }
  }

  @Override
  public void checkSessionValidity() throws SleetException {
    validateSessionStarted();
  }

  private void validateSessionStarted() throws GeneratorSessionException {
    if (this.timeCalc == null) {
      throw new GeneratorSessionException("Session was not started.  Start session by calling beginIdSession()");
    }
  }

  public void sleepUntilNextTimeValue() throws TimeCalculationException {
    long currValue = this.timeCalc.timeValue();
    long lastValue = currValue;
    while (currValue <= lastValue) {
      try {
        Thread.sleep(Math.max(this.timeCalc.getGranularity(), this.timeCalc.getGranularity() * Math.abs(lastValue - currValue)));
      } catch (InterruptedException e) {
        // ignore
      }
      currValue = this.timeCalc.timeValue();
    }
  }
}
