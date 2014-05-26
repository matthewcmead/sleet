package sleet.id;

public class TimeId implements TimeIdType {
  private final long value;
  private final TimeIdError error;
  private final int numBits;

  public TimeId(long value, TimeIdError error, int numBits) {
    this.value = value;
    this.error = error;
    this.numBits = numBits;
  }

  @Override
  public Long getId() {
    return this.value;
  }

  @Override
  public TimeIdError getError() {
    return this.error;
  }

  @Override
  public int getNumBitsInId() {
    return this.numBits;
  }

}
