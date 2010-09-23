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

import java.util.HashMap;
import java.util.Map;

import com.tdunning.plume.DoFn;
import com.tdunning.plume.PCollection;

public class MultipleParallelDo<T> extends DeferredOp {

  PCollection<T> origin;
  Map<PCollection<?>, DoFn<T, ?>> dests;
  
  public MultipleParallelDo(PCollection<T> origin) {
    this.origin = origin;
  }
  
  public <V> void addDest(DoFn<T, V> function, PCollection<V> dest) {
    if(dests == null) {
      dests = new HashMap<PCollection<?>, DoFn<T, ?>>();
    }
    dests.put(dest, function);
  }

  public PCollection<T> getOrigin() {
    return origin;
  }

  public Map<PCollection<?>, DoFn<T, ?>> getDests() {
    return dests;
  }
  
  @Override
  public String toString() {
    return "MultipleParallelDo, origin " + origin + " Dests " + dests;
  }
}
