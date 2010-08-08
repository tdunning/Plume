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
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.op.DeferredOp;
import com.tdunning.plume.local.lazy.op.Flatten;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.MultipleParallelDo;
import com.tdunning.plume.local.lazy.op.ParallelDo;

/**
 * Dummy executor that goes down-top by using recursive formulas and stores all intermediate results in-memory. 
 */
public class Executor {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T> Iterable<T> execute(LazyCollection<T> output) {
    if (output.isMaterialized()) {
      return output.getData(); // nothing else to execute
    } else {
      DeferredOp op = output.getDeferredOp();
      final List<T> result = Lists.newArrayList();
      // Flatten op
      if(op instanceof Flatten) {
        Flatten<T> flatten = (Flatten<T>)op;
        for(PCollection<T> col: flatten.getOrigins()) {
          Iterable<T> res = execute((LazyCollection<T>) col );
          result.addAll(Lists.newArrayList(res));
        }
        return result; // done with it
      }
      Iterable parent;
      EmitFn<T> emitter = new EmitFn<T>() {
        @Override
        public void emit(T v) {
          result.add(v);
        }
      };
      // ParallelDo
      if (op instanceof ParallelDo) {
        ParallelDo pDo = (ParallelDo) op;
        parent = execute((LazyCollection)pDo.getOrigin());
        for (Object obj : parent) {
          pDo.getFunction().process(obj, emitter);
        }
      // MultipleParallelDo -> parallel operations that read the same collection
      // In this version of executor, we will only compute the current collection, not its neighbors
      } else if(op instanceof MultipleParallelDo) {
        MultipleParallelDo mPDo = (MultipleParallelDo) op;
        parent = execute((LazyCollection)mPDo.getOrigin());
        DoFn function = (DoFn)mPDo.getDests().get(output); // get the function that corresponds to this collection
        for (Object obj : parent) {
          function.process(obj, emitter);
        }
      // GroupByKey
      } else if(op instanceof GroupByKey) {
        GroupByKey gBK = (GroupByKey) op;
        parent = execute(gBK.getOrigin());
        Map<Object, List> groupMap = Maps.newHashMap();
        // Perform in-memory group by operation
        for (Object obj : parent) {
          Pair p = (Pair)obj;
          List list = groupMap.get(p.getKey());
          if(list == null) {
            list = new ArrayList();
          }
          list.add(p.getValue());
          groupMap.put(p.getKey(), list);
        }
        for (Map.Entry<Object, List> entry: groupMap.entrySet()) {
          result.add((T)new Pair(entry.getKey(), entry.getValue()));
        }
      } 
      return result;
    }
  }
}
