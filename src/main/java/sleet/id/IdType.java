package sleet.id;

public interface IdType<T, E extends IdError> {
  public T getId();

  public E getError();

  public int getNumBitsInId();
}
