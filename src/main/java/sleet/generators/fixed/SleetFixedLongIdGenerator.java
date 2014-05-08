package sleet.generators.fixed;

import java.util.List;
import java.util.Properties;

import sleet.SleetException;
import sleet.generators.GeneratorConfigException;
import sleet.generators.IdGenerator;
import sleet.id.LongId;
import sleet.id.LongIdType;
import sleet.state.IdState;

public class SleetFixedLongIdGenerator implements IdGenerator<LongIdType> {
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
    this.value = new LongId(value);
  }

  @Override
  public void checkSessionValidity() throws SleetException {
    if (this.value == null) {
      
    }
  }

  @Override
  public void endIdSession() throws SleetException {
  }

  @Override
  public LongIdType getId(List<IdState<?>> states) throws SleetException {
    return this.value;
  }
}
