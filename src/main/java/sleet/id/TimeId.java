package sleet.id;

public class TimeId implements TimeIdType {
  final long value;
  final TimeIdError error;
  
  public TimeId(long value, TimeIdError error) {
    this.value = value;
    this.error = error;
  }

  @Override
  public Long getId() {
    return this.value;
  }

  @Override
  public TimeIdError getError() {
    return this.error;
  }

}
