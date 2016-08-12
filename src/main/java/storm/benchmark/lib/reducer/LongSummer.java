/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package storm.benchmark.lib.reducer;


public class LongSummer implements Reducer<Long> {

  private static final long serialVersionUID = -1102373670176409091L;

  @Override
  public Long reduce(Long v1, Long v2) {
    return v1 + v2;
  }

  @Override
  public Long zero() {
    return 0L;
  }

  @Override
  public boolean isZero(Long aLong) {
    return 0L == aLong;
  }
}
