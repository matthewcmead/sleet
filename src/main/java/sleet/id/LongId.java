package sleet.id;

public class LongId implements LongIdType {
  private final long value;
  private final int numBits;
  private final IdError error;

  public LongId(long value, IdError error, int numBits) {
    this.value = value;
    this.error = error;
    this.numBits = numBits;
  }

  @Override
  public Long getId() {
    return this.value;
  }

  @Override
  public IdError getError() {
    return this.error;
  }

  @Override
  public int getNumBitsInId() {
    return this.numBits;
  }

}
