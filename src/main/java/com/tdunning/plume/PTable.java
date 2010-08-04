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

import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.PTableType;

/**
 * Parallel table that supports map and reduce operations.
 */
public interface PTable<K, V> extends PCollection<Pair<K, V>>, Iterable<Pair<K, V>> {

  /**
   * Performs a function on each element of a parallel table returning a collection of values.
   *
   * @param fn The function to perform.
   * @return A parallel collection whose content is the result of applying fn to each element of
   *         this.
   */
  public <R>PCollection<R> map(DoFn<Pair<K, V>, R> fn, PCollectionType type);

  /**
   * Performs an operation on each element of a collection returning a transformed table.
   *
   * @param fn The function to perform on key/value pairs.
   * @return A parallel table containing the transformed data.
   */
  public <K1, V1>PTable<K1, V1> map(DoFn<Pair<K, V>, Pair<K1, V1>> fn, PTableType type);

  /**
   * Groups the elements of a table by key returning a new table with the same keys, but all values
   * for the same key grouped together.
   *
   * @return The grouped table.
   */
  public PTable<K, Iterable<V>> groupByKey();

  /**
   * Groups the elements of a table by key returning a new table with the same keys, but all values
   * for the same key grouped together and in the order specified by the ordering.
   *
   * @param order Determines the ordering of the values for each key
   * @return
   */
  public PTable<K, Iterable<V>> groupByKey(Ordering<V> order);

  // TODO how can we state that V is Iterable<X> for this one method?

  /**
   * Applies (possibly recursively) an associative function to elements of lists contained in a
   * table.
   *
   * @param fn The combination function transformation.
   * @return A table containing the combined values.
   */
  public <X> PTable<K, X> combine(CombinerFn<X> fn);

  // derived operations

  public <V2> PTable<K, Tuple2<Iterable<V>, Iterable<V2>>> join(PTable<K, V2> other);
}
