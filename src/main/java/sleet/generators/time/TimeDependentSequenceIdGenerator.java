package sleet.generators.time;

import java.util.List;
import java.util.Properties;

import sleet.SleetException;
import sleet.generators.GeneratorConfigException;
import sleet.generators.GeneratorSessionException;
import sleet.generators.IdGenerator;
import sleet.id.LongIdType;
import sleet.id.TimeIdType;
import sleet.state.IdState;

public class TimeDependentSequenceIdGenerator implements IdGenerator<LongIdType> {
  public static final String BITS_IN_SEQUENCE_KEY = "sequence.bits.in.sequence.value";

  private long maxSequenceValue = -1;
  private long lastTimeValue = -1;

  @Override
  public void beginIdSession(Properties config) throws SleetException {
    String bitsStr = config.getProperty(BITS_IN_SEQUENCE_KEY);
    if (bitsStr == null) {
      throw new GeneratorConfigException("Missing number of bits for the sequence value, must be specified in configuration properties key \"" + BITS_IN_SEQUENCE_KEY + "\".");
    }
    long bits = -1;
    try {
      bits = Long.valueOf(bitsStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse number of bits from value \"" + bitsStr + "\".  The value for configuration properties key \"" + BITS_IN_SEQUENCE_KEY + "\" must be a long.");
    }

    this.maxSequenceValue = (1L << bits) - 1L;
  }

  @Override
  public void checkSessionValidity() throws SleetException {
    validateSessionStarted();
  }

  @Override
  public void endIdSession() throws SleetException {
    validateSessionStarted();
    this.maxSequenceValue = -1;
  }

  @Override
  public LongIdType getId(List<IdState<?>> states) throws SleetException {
    validateSessionStarted();
    TimeIdType timeIdType = null; 
    for (IdState<?> state : states) {
      if (TimeIdType.class.isAssignableFrom(state.getGeneratorClass())) {
        if (timeIdType == null) {
          timeIdType = (TimeIdType) state.getId();
        } else {
          throw new TimeDependencyException(this.getClass().getName() + " depends on there being a single preceeding TimeIdType id, but found at least two.");
        }
      }
    }
    if (timeIdType == null) {
      throw new TimeDependencyException(this.getClass().getName() + " depend on there being a single preceeding TimeIdTYpe id, but found none.");
    }
    
    /**
     * TODO MCM examine the time id, generate a new id in the sequence or block until a new time unit occurs
     */
    
    return null;
  }

  private void validateSessionStarted() throws GeneratorSessionException {
    if (this.maxSequenceValue == -1) {
      throw new GeneratorSessionException("Session was not started.  Start session by calling beginIdSession()");
    }
  }

}
