package sleet.id;

public class LongId implements LongIdType {
  final long value;
  
  public LongId(long value) {
    this.value = value;
  }

  @Override
  public Long getId() {
    return this.value;
  }

}
