package sleet.generators.time;

import sleet.id.BaseIdError;


public class SequenceIdOverflowError extends BaseIdError {

  public SequenceIdOverflowError(String message) {
    super(message);
  }
}
