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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.tdunning.plume.CollectionConversion;
import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.Ordering;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.TableConversion;
import com.tdunning.plume.Tuple2;
import com.tdunning.plume.local.lazy.op.DeferredOp;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.ParallelDoTC;
import com.tdunning.plume.local.lazy.op.ParallelDoTT;

/**
 * A LazyTable that can be either materialized or unmaterialized. Unmaterialized
 * tables have a reference to the {@link DeferredOp} that creates them.
 * 
 * @author pere
 * 
 * @param <K>
 * @param <V>
 */
public class LazyTable<K, V> extends LazyCollection<Pair<K, V>> implements PTable<K, V> {

  boolean materialized = false;
  private List<Pair<K, V>> data;

  DeferredOp deferredOp;
  List<DeferredOp> downOps;

  protected void addDownOp(DeferredOp op) {
    if (downOps == null) {
      downOps = new ArrayList<DeferredOp>();
    }
    downOps.add(op);
  }

  @Override
  public Iterator<Pair<K, V>> iterator() {
    if (materialized) {
      return data.iterator();
    } else {
      throw new UnsupportedOperationException(
          "Can't iterate over unmaterialized PTable");
    }
  }

  /**
   * Creates a new LazyCollection from a {@link ParallelDoTC} deferred operation
   * which maps a PTable to a PCollection
   */
  @Override
  public <R> PCollection<R> map(DoFn<Pair<K, V>, R> fn,
      CollectionConversion<R> conversion) {
    LazyCollection<R> dest = new LazyCollection<R>();
    ParallelDoTC<K, V, R> op = new ParallelDoTC<K, V, R>(this, dest, fn);
    dest.deferredOp = op;
    addDownOp(op);
    return dest;
  }

  /**
   * Creates a new LazyTable from a {@link ParallelDoTT} deferred operation
   * which maps a PTable to another PTable
   */
  @Override
  public <K1, V1> PTable<K1, V1> map(DoFn<Pair<K, V>, Pair<K1, V1>> fn,
      TableConversion<K1, V1> conversion) {
    LazyTable<K1, V1> dest = new LazyTable<K1, V1>();
    ParallelDoTT<K, V, K1, V1> op = new ParallelDoTT<K, V, K1, V1>(this, dest,
        fn);
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
    GroupByKey<K, V> groupByKey = new GroupByKey<K, V>(this, dest);
    dest.deferredOp = groupByKey;
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

  public boolean isMaterialized() {
    return materialized;
  }

  public List<Pair<K, V>> getData() {
    return data;
  }

  public DeferredOp getDeferredOp() {
    return deferredOp;
  }

  public List<DeferredOp> getDownOps() {
    return downOps;
  }
}
