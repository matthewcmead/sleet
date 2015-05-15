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
package sleet.utils.zookeeper;

import java.io.IOException;

public abstract class InstanceIdManager {

  /**
   * Gets the max number of instances.
   * 
   * @return the max number of instances.
   */
  public abstract int getMaxNumberOfInstances();

  /**
   * Attempts to get an instance id.
   * 
   * @return If successful the return value will be greater than or equal 0. If
   *         not successful < 0.
   * @throws IOException
   */
  public abstract int tryToGetId(long millisToWait) throws IOException;
  public abstract boolean sessionValid(boolean allowValidityStateCaching) throws IOException;
  public abstract int getCurrentId() throws IOException;
  public abstract void releaseId(int id) throws IOException;

}
