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
import sleet.utils.time.TimeCalculator;

/**
 * TODO MCM detect backward clock skew and either throw exception or handle, depending on configuration
 * @author mmead
 *
 */
public class TimeIdGenerator implements IdGenerator<TimeIdType> {
  private TimeCalculator timeCalc;
  
  /**
   * TODO MCM handle configuration for what to do with backward clock skew
   */
  public static final String EPOCH_KEY = "time.epoch.in.java.system.time.millis";
  public static final String GRANULARITY_IN_MS_KEY = "time.granularity.milliseconds";
  public static final String BITS_IN_TIME_VALUE_KEY = "time.bits.in.time.value";
  
  @Override
  public void endIdSession() throws SleetException {
    validateSessionStarted();
    this.timeCalc = null;
  }

  @Override
  public void beginIdSession(Properties config) throws SleetException {
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
    /**
     * TODO MCM Should we validate that it isn't in the future or negative?
     */
    String granularityStr = config.getProperty(GRANULARITY_IN_MS_KEY);
    if (granularityStr == null) {
      throw new GeneratorConfigException("Missing granularity (in milliseconds), must be specified in configuration properties key \"" + GRANULARITY_IN_MS_KEY + "\".");
    }
    long granularityMs = -1;
    try {
      granularityMs = Long.valueOf(granularityStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse granularity (in milliseconds) from value \"" + granularityStr + "\".  The value for configuration properties key \"" + GRANULARITY_IN_MS_KEY + "\" must be a long.");
    }
    /**
     * TODO MCM Should we validate that it isn't negative?
     */
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
  }

  @Override
  public TimeIdType getId(List<IdState<?>> states) throws SleetException {
    validateSessionStarted();
    return new TimeId(this.timeCalc.timeValue());
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

}
