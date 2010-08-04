package com.tdunning.plume.local.lazy.op;

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

import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.LazyTable;

public class GroupByKey<K, V> extends OneToOneOp<Pair<K, V>, Pair<K, Iterable<V>>> {

  LazyTable<K, V> origin;
  LazyTable<K, Iterable<V>> dest;

  public GroupByKey(LazyTable<K, V> origin, LazyTable<K, Iterable<V>> dest) {
    this.origin = origin;
    this.dest = dest;
  }

  public LazyTable<K, Iterable<V>> getDest() {
    return dest;
  }

  public LazyTable<K, V> getOrigin() {
    return origin;
  }
}
