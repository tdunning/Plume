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

import java.util.List;

import com.google.common.collect.Lists;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.local.lazy.op.DeferredOp;
import com.tdunning.plume.local.lazy.op.Flatten;
import com.tdunning.plume.local.lazy.op.ParallelDoCC;
import com.tdunning.plume.local.lazy.op.ParallelDoTC;

/**
 * Dummy executor that goes down-top by using recursive formulas and stores all intermediate results in-memory. 
 * 
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
      // ParallelDo PCollection -> PCollection
      if (op instanceof ParallelDoCC) {
        ParallelDoCC pDo = (ParallelDoCC) op;
        parent = execute((LazyCollection) pDo.getOrigin());
        for (Object obj : parent) {
          pDo.getFunction().process(obj, emitter);
        }
      // ParallelDo PTable -> PCollection
      } else if (op instanceof ParallelDoTC) {
        throw new RuntimeException("Not yet implemented");
      }
      return result;
    }
  }
}
