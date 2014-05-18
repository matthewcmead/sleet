package sleet.generators.time;

import sleet.id.BaseIdError;
import sleet.id.TimeIdError;

public class TimeIdReverseSkewError extends BaseIdError implements TimeIdError {
  private final long deltaMs;

  public TimeIdReverseSkewError(String message) {
    super(message);
    this.deltaMs = -1;
  }

  public TimeIdReverseSkewError(String message, long deltaMs) {
    super(message);
    this.deltaMs = deltaMs;
  }

  public long getDeltaMs() {
    return deltaMs;
  }
}
