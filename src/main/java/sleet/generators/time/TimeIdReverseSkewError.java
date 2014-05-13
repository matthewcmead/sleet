package sleet.generators.time;

import sleet.id.BaseIdError;
import sleet.id.TimeIdError;

public class TimeIdReverseSkewError extends BaseIdError implements TimeIdError {

  public TimeIdReverseSkewError(String message) {
    super(message);
  } 

}
