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
 * A pair of values, generally used as a key and value as the input or output of
 * a DoFn in the case where the input or output respectively are a PTable.
 *
 * IF cons'ing lots of Pairs becomes a problem, we may need to make this class
 * final to allow better compiler optimizations.
 */
public class Pair<K, V> {
  private K key;
  private V value;

  public Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public static <V1, V2> Pair<V1, V2> create(V1 x1, V2 x2) {
    return new Pair<V1, V2>(x1, x2);
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "Pair{" +
            "key=" + key +
            ", value=" + value +
            '}';
  }
}
