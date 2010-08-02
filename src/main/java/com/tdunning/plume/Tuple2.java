/*
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

package com.tdunning.plume;

/**
 * Container for two objects used during joins.  It is an open question whether a Pair should just
 * be a Tuple2 (or vice versa).
 */
public class Tuple2<V0, V1> {
  private V0 v0;
  private V1 v1;

  private Tuple2(V0 v0, V1 v1) {
    this.v0 = v0;
    this.v1 = v1;
  }

  public static <V0, V1> Tuple2<V0, V1> create(V0 v0, V1 v1) {
    return new Tuple2<V0, V1>(v0, v1);
  }

  public V0 get0() {
    return v0;
  }

  public V1 get1() {
    return v1;
  }
}
