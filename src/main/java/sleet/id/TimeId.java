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
package sleet.id;

public class TimeId implements TimeIdType {
  private final long value;
  private final TimeIdError error;
  private final int numBits;

  public TimeId(long value, TimeIdError error, int numBits) {
    this.value = value;
    this.error = error;
    this.numBits = numBits;
  }

  @Override
  public Long getId() {
    return this.value;
  }

  @Override
  public TimeIdError getError() {
    return this.error;
  }

  @Override
  public int getNumBitsInId() {
    return this.numBits;
  }

}
