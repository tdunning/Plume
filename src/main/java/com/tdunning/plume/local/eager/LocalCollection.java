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
import com.tdunning.plume.*;
import com.tdunning.plume.types.PType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
* Completely local version of a PCollection.
*/
public class LocalCollection<T> implements PCollection<T> {
  private List<T> data = Lists.newArrayList();

  @Override
  public <R> PCollection<R> map(DoFn<T, R> fn, PType type) {
    final LocalCollection<R> r = new LocalCollection<R>();
    for (T t : data) {
      fn.process(t, new EmitFn<R>() {
        @Override
        public void emit(R y) {
          r.data.add(y);
        }
      });
    }
    return r;
  }

  @Override
  public <K, V> PTable<K, V> map(DoFn<T, Pair<K, V>> fn, PType type) {
    final LocalTable<K, V> r = new LocalTable<K, V>();
    for (final T t : data) {
      fn.process(t, new EmitFn<Pair<K, V>>() {
        @Override
        public void emit(Pair<K, V> value) {
          r.getData().add(value);
        }
      });
    }
    return r;
  }

  @Override
  public PTable<T, Integer> count() {
    Map<T, Integer> x = Maps.newHashMap();
    for (T t : data) {
      Integer v = x.get(t);
      if (v == null) {
        x.put(t, 1);
      } else {
        x.put(t, v + 1);
      }
    }
    LocalTable<T, Integer> r = new LocalTable<T, Integer>();
    for (T t : x.keySet()) {
      r.getData().add(new Pair<T, Integer>(t, x.get(t)));
    }
    return r;
  }

  public static <X> LocalCollection<X> wrap(Iterable<X> data) {
    return new LocalCollection<X>().addAll(data);
  }

  public LocalCollection<T> addAll(Iterable<T> data) {
    for (T t : data) {
      this.data.add(t);
    }
    return this;
  }

  public List<T> getData() {
    return data;
  }

  /**
   * Returns an iterator over a set of elements of type T.
   *
   * @return an Iterator.
   */
  public Iterator<T> iterator() {
    return data.iterator();
  }
}
