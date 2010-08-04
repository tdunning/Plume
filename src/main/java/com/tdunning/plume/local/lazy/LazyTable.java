package com.tdunning.plume.local.lazy;

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

import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.Ordering;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.Tuple2;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.ParallelDo;
import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.PTableType;

public class LazyTable<K, V> extends LazyCollection<Pair<K, V>> implements PTable<K, V> {

  public LazyTable() {
  }

  /**
   * Creates a new LazyCollection from a {@link ParallelDoTC} deferred operation
   * which maps a PTable to a PCollection
   */
  @Override
  public <R> PCollection<R> map(DoFn<Pair<K, V>, R> fn, PCollectionType type) {
    LazyCollection<R> dest = new LazyCollection<R>();
    ParallelDo op = new ParallelDo(fn, this, dest);
    dest.deferredOp = op;
    addDownOp(op);
    return dest;
  }

  /**
   * Creates a new LazyTable from a {@link ParallelDoTT} deferred operation
   * which maps a PTable to another PTable
   */
  @Override
  public <K1, V1> PTable<K1, V1> map(DoFn<Pair<K, V>, Pair<K1, V1>> fn, PTableType type) {
    LazyTable<K1, V1> dest = new LazyTable<K1, V1>();
    ParallelDo op = new ParallelDo(fn, this, dest);
    dest.deferredOp = op;
    addDownOp(op);
    return dest;
  }

  /**
   * Creates a new PTable from a {@link ParallelDoTT} deferred operation
   */
  @Override
  public PTable<K, Iterable<V>> groupByKey() {
    LazyTable<K, Iterable<V>> dest = new LazyTable<K, Iterable<V>>();
    dest.deferredOp = new GroupByKey<K, V>(this, dest);
    addDownOp(deferredOp);
    return dest;
  }

  /**
   * TODO
   */
  @Override
  public PTable<K, Iterable<V>> groupByKey(Ordering<V> order) {
    throw new UnsupportedOperationException("Net yet implemented");
  }

  /**
   * TODO
   */
  @Override
  public <X> PTable<K, X> combine(CombinerFn<X> fn) {
    throw new UnsupportedOperationException("Net yet implemented");
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
