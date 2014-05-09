package sleet.generators.fixed;

import java.util.List;
import java.util.Properties;

import sleet.SleetException;
import sleet.generators.GeneratorConfigException;
import sleet.generators.GeneratorSessionException;
import sleet.generators.IdGenerator;
import sleet.id.IdError;
import sleet.id.LongId;
import sleet.id.LongIdType;
import sleet.state.IdState;

public class FixedLongIdGenerator implements IdGenerator<LongIdType> {
  public static final String FIXED_LONG_VALUE_KEY = "fixed.long.value";
  
  private LongId value = null;

  @Override
  public void beginIdSession(Properties config) throws SleetException {
    String valueStr = config.getProperty(FIXED_LONG_VALUE_KEY);
    if (valueStr == null) {
      throw new GeneratorConfigException("Missing value for fixed long id generation, must be specified in configuration properties key \"" + FIXED_LONG_VALUE_KEY + "\".");
    }
    long value = -1;
    try {
      value = Long.valueOf(valueStr);
    } catch (NumberFormatException e) {
      throw new GeneratorConfigException("Failed to parse long value from value \"" + valueStr + "\".  The value for configuration properties key \"" + FIXED_LONG_VALUE_KEY + "\" must be a long.");
    }
    this.value = new LongId(value, null);
  }

  @Override
  public void checkSessionValidity() throws SleetException {
    validateSessionStarted();
  }

  @Override
  public void endIdSession() throws SleetException {
    validateSessionStarted();
  }

  @Override
  public LongIdType getId(List<IdState<?, ?>> states) throws SleetException {
    validateSessionStarted();
    return this.value;
  }

  private void validateSessionStarted() throws GeneratorSessionException {
    if (this.value == null) {
      throw new GeneratorSessionException("Session was not started.  Start session by calling beginIdSession()");
    }
  }
}
