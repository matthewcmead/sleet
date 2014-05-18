package sleet.generators.time;

import java.util.List;
import java.util.Properties;

import sleet.SleetException;
import sleet.generators.GeneratorConfigException;
import sleet.generators.GeneratorSessionException;
import sleet.generators.IdGenerator;
import sleet.id.LongId;
import sleet.id.LongIdType;
import sleet.id.TimeIdType;
import sleet.state.IdState;

public class TimeDependentSequenceIdGenerator implements IdGenerator<LongIdType> {
  public static final String BITS_IN_SEQUENCE_KEY = "sequence.bits.in.sequence.value";

  private long maxSequenceValue = -1;
  private long lastTimeValue = -1;

  private Object lock = new Object();
  private long sequenceValue = 0;

  @Override
  public void beginIdSession(Properties config) throws SleetException {
    if (this.maxSequenceValue != -1) {
      throw new GeneratorSessionException("Session was already started.  Stop session by calling endIdSession() then start session by calling beginIdSession()");
    }
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
  public LongIdType getId(List<IdState<?, ?>> states) throws SleetException {
    validateSessionStarted();
    TimeIdType timeIdType = null;
    for (IdState<?, ?> state : states) {
      if (TimeIdType.class.isAssignableFrom(state.getGeneratorClass())) {
        if (timeIdType == null) {
          timeIdType = (TimeIdType) state.getId();
        } else {
          throw new TimeDependencyException(this.getClass().getName() + " depends on there being a single preceeding TimeIdType id, but found at least two.");
        }
      }
    }
    if (timeIdType == null) {
      throw new TimeDependencyException(this.getClass().getName() + " depend on there being a single preceeding TimeIdType id, but found none.");
    }

    long currentTimeValue = timeIdType.getId();
    long returnValue = -1;
    synchronized (lock) {
      if (currentTimeValue < this.lastTimeValue) {
        return new LongId(-1, new TimeIdReverseSkewError(this.getClass().getName()
            + " depends on the preceeding id generator which generated the TimeIdType to guard against the TimeIdType values decreasing over time"));
      } else if (currentTimeValue == this.lastTimeValue) {
        if (this.sequenceValue > this.maxSequenceValue || this.sequenceValue < 0) {
          return new LongId(-1, new SequenceIdOverflowError(this.getClass().getName() + " overflowed the maximum sequence value when allocating a sequence id for time value \"" + currentTimeValue
              + "\"."));
        } else {
          returnValue = this.sequenceValue;
          this.sequenceValue++;
        }
      } else {
        this.sequenceValue = 0;
        returnValue = this.sequenceValue;
        this.sequenceValue++;
      }
    }
    return new LongId(returnValue, null);
  }

  private void validateSessionStarted() throws GeneratorSessionException {
    if (this.maxSequenceValue == -1) {
      throw new GeneratorSessionException("Session was not started.  Start session by calling beginIdSession()");
    }
  }

}
