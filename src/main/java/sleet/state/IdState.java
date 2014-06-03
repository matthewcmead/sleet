/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
