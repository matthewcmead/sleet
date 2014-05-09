package sleet.state;

import sleet.generators.IdGenerator;
import sleet.id.IdError;
import sleet.id.IdType;

public class IdState<T, E extends IdError> {
  private Class<? extends IdGenerator<? extends IdType<T, E>>> generatorClass;
  private IdType<T, E> id;


  public IdState(Class<? extends IdGenerator<? extends IdType<T, E>>> generatorClass, IdType<T, E> id) {
    this.generatorClass = generatorClass;
    this.id = id;
  }

  public Class<? extends IdGenerator<? extends IdType<T, E>>> getGeneratorClass() {
    return generatorClass;
  }

  public IdType<T, E> getId() {
    return id;
  }
}
