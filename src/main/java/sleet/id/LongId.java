package sleet.id;

public class LongId implements LongIdType {
  final long value;
  final IdError error;
  
  public LongId(long value, IdError error) {
    this.value = value;
    this.error = error;
  }

  @Override
  public Long getId() {
    return this.value;
  }

  @Override
  public IdError getError() {
    return this.error;
  }

}
