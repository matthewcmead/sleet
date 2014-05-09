package sleet.generators;

import java.util.List;
import java.util.Properties;

import sleet.SleetException;
import sleet.id.IdType;
import sleet.state.IdState;

public interface IdGenerator<T extends IdType<?, ?>> {
  public void beginIdSession(Properties config) throws SleetException;
  public void checkSessionValidity() throws SleetException;
  public void endIdSession() throws SleetException;
  public IdType<?, ?> getId(List<IdState<?, ?>> states) throws SleetException;
}
