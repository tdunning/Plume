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

package com.tdunning.plume.local.eager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.tdunning.plume.types.PType;
import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.Ordering;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Completely local eager version of a PTable.
 */
public class LocalTable<K, V> extends LocalCollection<Pair<K, V>> implements PTable<K, V> {

  private List<Pair<K, V>> data = Lists.newArrayList();

  /**
   * Performs a function on each element of a parallel table returning a collection of values.
   *
   * @param fn The function to perform.
   * @return A parallel collection whose content is the result of applying fn to each element of
   *         this.
   */
  @Override
  public <R> PCollection<R> map(DoFn<Pair<K, V>, R> fn, PType type) {
    final LocalCollection<R> r = new LocalCollection<R>();
    for (Pair<K, V> v : data) {
      fn.process(v, new EmitFn<R>() {
        @Override
        public void emit(R y) {
          r.getData().add(y);
        }
      });
    }
    return r;
  }


  /**
   * Performs an operation on each element of a collection returning a transformed table.
   *
   * @param fn The function to perform on key/value pairs.
   * @return A parallel table containing the transformed data.
   */
  @Override
  public <K1, V1> PTable<K1, V1> map(DoFn<Pair<K, V>, Pair<K1, V1>> fn, PType type) {
    final LocalTable<K1, V1> r = new LocalTable<K1, V1>();
    for (Pair<K, V> v : data) {
      fn.process(v, new EmitFn<Pair<K1, V1>>() {
        @Override
        public void emit(Pair<K1, V1> value) {
          r.getData().add(value);
        }

      });
    }
    return r;
  }

  /**
   * Groups the elements of a table by key returning a new table with the same keys, but all values
   * for the same key grouped together.
   *
   * @return The grouped table.
   */
  @Override
  public PTable<K, Iterable<V>> groupByKey() {
    // can't use a guava multimap here because identical key,value pairs would be suppressed.
    Map<K, List<V>> r = Maps.newHashMap();
    for (Pair<K, V> v : data) {
      List<V> values = r.get(v.getKey());
      if (values == null) {
        values = Lists.newArrayList();
        r.put(v.getKey(), values);
      }
      values.add(v.getValue());
    }
    return LocalTable.wrap(r);
  }

  private static <K, V> PTable<K, Iterable<V>> wrap(Map<K, List<V>> data) {
    LocalTable<K, Iterable<V>> r = new LocalTable<K, Iterable<V>>();
    List<Pair<K, Iterable<V>>> list = r.getData();
    for (K k : data.keySet()) {
      list.add(Pair.<K, Iterable<V>>create(k, data.get(k)));
    }
    return r;
  }

  /**
   * Groups the elements of a table by key returning a new table with the same keys, but all values
   * for the same key grouped together and in the order specified by the ordering.
   *
   * @return A table of keys and groups.
   */
  @Override
  public PTable<K, Iterable<V>> groupByKey(Ordering<V> order) {
    // TODO look into a better argument type here
    return null;
  }

  /**
   * Applies (possibly recursively) an associative function to elements of lists contained in a
   * table.
   *
   * @return A table containing the combined values.
   */
  @Override
  public <X> PTable<K, X> combine(CombinerFn<X> combiner) {
    final LocalTable<K, X> r = new LocalTable<K, X>();
    for (final Pair<K, V> x : data) {
      @SuppressWarnings({"unchecked"}) Iterable<X> v = (Iterable<X>) x.getValue();
      r.getData().add(Pair.create(x.getKey(), combiner.combine(v)));
    }
    return r;
  }

  @Override
  public <V2> PTable<K, Tuple2<Iterable<V>, Iterable<V2>>> join(PTable<K, V2> other) {
    Map<K, List<V>> m0 = Maps.newHashMap();
    for (Pair<K, V> kvPair : data) {
      List<V> v = m0.get(kvPair.getKey());
      if (v == null) {
        v = Lists.newArrayList();
        m0.put(kvPair.getKey(), v);
      }
      v.add(kvPair.getValue());
    }
    Map<K, List<V2>> m1 = Maps.newHashMap();
    for (Pair<K, V2> kvPair : ((LocalTable<K, V2>) other).getData()) {
      List<V2> v = m1.get(kvPair.getKey());
      if (v == null) {
        v = Lists.newArrayList();
        m1.put(kvPair.getKey(), v);
      }
      v.add(kvPair.getValue());
    }

    LocalTable<K, Tuple2<Iterable<V>, Iterable<V2>>> z = new LocalTable<K, Tuple2<Iterable<V>, Iterable<V2>>>();
    for (K k : m0.keySet()) {
      Iterable<V> v0 = m0.get(k);
      Iterable<V2> v1 = m1.get(k);
      if (v1 == null) {
        v1 = Lists.newArrayList();
      } else {
        m1.remove(k);
      }
      z.getData().add(Pair.create(k, Tuple2.create(v0, v1)));
    }

    for (K k : m1.keySet()) {
      List<V2> v1 = m1.get(k);
      List<V> v0 = m0.get(k);
      if (v0 == null) {
        v0 = Lists.newArrayList();
      }
      z.getData().add(Pair.create(k, Tuple2.<Iterable<V>, Iterable<V2>>create(v0, v1)));
    }
    return z;
  }


  public List<Pair<K, V>> getData() {
    return data;
  }

  /**
   * Returns an iterator over a set of elements of type T.
   *
   * @return an Iterator.
   */
  public Iterator<Pair<K, V>> iterator() {
    return data.iterator();
  }
}
