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

package com.tdunning.plume.local.lazy;

import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.Ordering;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.Tuple2;
import com.tdunning.plume.local.lazy.op.CombineValues;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.ParallelDo;
import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.PTableType;

public class LazyTable<K, V> extends LazyCollection<Pair<K, V>> implements PTable<K, V> {

  public LazyTable() {
  }

  public LazyTable(Iterable<Pair<K, V>> data) {
    super(data);
  }
  
  /**
   * Creates a new LazyCollection from a {@link ParallelDo} deferred operation
   * which maps a PTable to a PCollection
   */
  @Override
  public <R> PCollection<R> map(DoFn<Pair<K, V>, R> fn, PCollectionType type) {
    LazyCollection<R> dest = new LazyCollection<R>();
    ParallelDo<Pair<K, V>, R> op = new ParallelDo<Pair<K, V>, R>(fn, this, dest);
    dest.deferredOp = op;
    addDownOp(op);
    return dest;
  }

  /**
   * Creates a new LazyTable from a {@link ParallelDo} deferred operation
   * which maps a PTable to another PTable
   */
  @Override
  public <K1, V1> PTable<K1, V1> map(DoFn<Pair<K, V>, Pair<K1, V1>> fn, PTableType type) {
    LazyTable<K1, V1> dest = new LazyTable<K1, V1>();
    ParallelDo<Pair<K, V>, Pair<K1, V1>> op = new ParallelDo<Pair<K, V>, Pair<K1, V1>>(fn, this, dest);
    dest.deferredOp = op;
    addDownOp(op);
    return dest;
  }

  /**
   * Creates a new PTable from a {@link ParallelDo} deferred operation
   */
  @Override
  public PTable<K, Iterable<V>> groupByKey() {
    LazyTable<K, Iterable<V>> dest = new LazyTable<K, Iterable<V>>();
    dest.deferredOp = new GroupByKey<K, V>(this, dest);
    addDownOp(deferredOp);
    return dest;
  }

  /**
   * Creates a new PTable from a {@link GroupByKey} deferred operation
   * TODO this looks wrong since it doesn't pay attention to the order
   */
  @Override
  public PTable<K, Iterable<V>> groupByKey(Ordering<V> order) {
    LazyTable<K, Iterable<V>> dest = new LazyTable<K, Iterable<V>>();
    GroupByKey<K, V> groupByKey = new GroupByKey<K, V>(this, dest);
    dest.deferredOp = groupByKey;
    addDownOp(groupByKey);
    return dest;
  }

  /**
   * Creates a new PTable from a {@link CombineValues} deferred operation
   */
  @Override
  public <X> PTable<K, X> combine(CombinerFn<X> fn) {
    LazyTable<K, X> dest = new LazyTable<K, X>();
    // TODO check how to do this better instead of unchecked casting
    CombineValues<K, X> combine = new CombineValues<K, X>(fn, (LazyTable<K, Iterable<X>>) this, dest);
    dest.deferredOp = combine;
    addDownOp(combine);
    return dest;
  }

  /**
   * TODO
   */
  @Override
  public <V2> PTable<K, Tuple2<Iterable<V>, Iterable<V2>>> join(
      PTable<K, V2> other) {
    throw new UnsupportedOperationException("Net yet implemented");
  }
}
