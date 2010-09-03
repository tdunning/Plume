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

package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;

public class CombineValues<K, V> extends ParallelDo<Pair<K, Iterable<V>>, Pair<K, V>> {
  
  CombinerFn<V> combiner; 

  public CombineValues(final CombinerFn<V> combiner,
      PCollection<Pair<K, Iterable<V>>> origin, PCollection<Pair<K, V>> dest) {
    super(new DoFn<Pair<K, Iterable<V>>, Pair<K, V>>() {
      @Override
      public void process(Pair<K, Iterable<V>> v, EmitFn<Pair<K, V>> emitter) {
        emitter.emit(Pair.create(v.getKey(), combiner.combine(v.getValue())));
      }
    } , origin, dest);
    this.combiner = combiner;
  }

  public CombinerFn<V> getCombiner() {
    return combiner;
  }
}
