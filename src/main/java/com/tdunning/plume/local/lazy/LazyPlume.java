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

import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.Plume;
import com.tdunning.plume.avro.AvroFile;
import com.tdunning.plume.local.eager.LocalCollection;
import com.tdunning.plume.local.lazy.op.Flatten;
import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.PTableType;
import com.tdunning.plume.types.PType;

/**
 * Runtime for Plume implementing deferred execution and optimization.
 */
public class LazyPlume extends Plume {

  @Override
  public PCollection<String> readTextFile(String name) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PCollection<String> readResourceFile(String name) throws IOException {
    return LocalCollection.wrap(Resources.readLines(Resources.getResource(name), Charsets.UTF_8));
  }

  @Override
  public <T> PCollection<T> readAvroFile(String name, PType<T> type) {
    return new AvroFile<T>(name, type);
  }
  
  @Override
  public <T> PCollection<T> fromJava(Iterable<T> source) {
    return new LazyCollection<T>(source);
  }


  public <T> PCollection<T> flatten(PCollectionType type, PCollection<T>... args) {
    LazyCollection<T> dest = new LazyCollection<T>();
    Flatten<T> flatten = new Flatten<T>(Lists.newArrayList(args), dest);
    dest.deferredOp = flatten;
    for(PCollection<T> col: args) {
      ((LazyCollection<T>)col).addDownOp(flatten);
    }
    return dest;
  }

  public <K, V> PTable<K, V> flatten(PTableType type, PCollection<Pair<K, V>>... args) {
    LazyTable<K, V> dest = new LazyTable<K, V>();
    Flatten<Pair<K, V>> flatten = new Flatten<Pair<K, V>>(Lists.newArrayList(args), dest);
    dest.deferredOp = flatten;
    for(PCollection<Pair<K, V>> col: args) {
      ((LazyCollection<Pair<K, V>>)col).addDownOp(flatten);
    }
    return dest;
  }

  @Override
  public <T> PCollection<T> flatten(PCollection<T>... args) {
    return flatten(null, args);
  }

  @Override
  public <T> void writeAvroFile(String name, PCollection<T> data, PType<T> type) {
    throw new RuntimeException("Not done");
  }
}
