package sleet.id;

public interface IdType<T, E> {
  public T getId();
  public E getError();
}
