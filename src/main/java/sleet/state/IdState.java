package sleet.state;

import sleet.generators.IdGenerator;
import sleet.id.IdType;

public class IdState<T> {
  private Class<? extends IdGenerator<? extends IdType<T>>> generatorClass;
  private IdType<T> id;


  public IdState(Class<? extends IdGenerator<? extends IdType<T>>> generatorClass, IdType<T> id) {
    this.generatorClass = generatorClass;
    this.id = id;
  }

  public Class<? extends IdGenerator<? extends IdType<T>>> getGeneratorClass() {
    return generatorClass;
  }

  public IdType<T> getId() {
    return id;
  }
}
