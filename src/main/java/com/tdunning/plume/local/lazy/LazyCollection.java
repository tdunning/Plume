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

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.op.DeferredOp;
import com.tdunning.plume.local.lazy.op.ParallelDo;
import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.PTableType;

/**
 * A LazyCollection that can be either materialized or unmaterialized. 
 * Unmaterialized collections have a reference to the {@link DeferredOp} that creates them.
 */
public class LazyCollection<T> implements PCollection<T> {

  boolean materialized = false;
  private List<T> data;
  private String file; // points to a file in local filesystem, if collection is materialized like that TODO to be better defined

  DeferredOp deferredOp;
  
  List<DeferredOp> downOps;

  /**
   * Build a PCollection with materialized state
   * 
   * @param data  Concrete data from which to build the PCollection.
   */
  public LazyCollection(Iterable<T> data) {
    this.data = Lists.newArrayList(data);
    materialized = true;
  }
  
  public List<T> getData() {
    return data;
  }

  /**
   * Unmaterialized PCollection constructor
   */
  LazyCollection() {
  }
  
  protected void addDownOp(DeferredOp op) {
    if(downOps == null) {
      downOps  = new ArrayList<DeferredOp>();
    }
    downOps.add(op);
  }

  @Override
  public Iterator<T> iterator() {
    if(materialized) {
      return data.iterator();
    } else {
      throw new UnsupportedOperationException("Can't iterate over unmaterialized PCollection");
    }
  }

  /**
   * Creates a new LazyCollection from a deferred operation
   * which maps a PCollection to another PCollection
   */
  @Override
  public <R> PCollection<R> map(DoFn<T, R> fn, PCollectionType type) {
    LazyCollection<R> dest = new LazyCollection<R>();
    ParallelDo<T, R> op = new ParallelDo<T, R>(fn, this, dest);
    dest.deferredOp = op;
    addDownOp(op);
    return dest;
  }

  /**
   * Creates a new LazyTable from a deferred operation
   * which maps a PCollection to a PTable
   */
  public <K, V> PTable<K, V> map(DoFn<T, Pair<K, V>> fn, PTableType type) {
    LazyTable<K, V> dest = new LazyTable<K, V>();
    ParallelDo<T, Pair<K, V>> op = new ParallelDo<T, Pair<K, V>>(fn, this, dest);
    dest.deferredOp = op;
    addDownOp(op);
    return dest;
  }
  
  public DeferredOp getDeferredOp() {
    return deferredOp;
  }

  public void setDeferredOp(DeferredOp deferredOp) {
    this.deferredOp = deferredOp;
  }

  public boolean isMaterialized() {
    return materialized;
  }

  public List<DeferredOp> getDownOps() {
    return downOps;
  }

  /**
   * TODO
   */
  @Override
  public PTable<T, Integer> count() {
    throw new UnsupportedOperationException("Net yet implemented");
  }
  
  public String getFile() {
    return file;
  }

  public void setFile(String file) {
    this.file = file;
  }
}
