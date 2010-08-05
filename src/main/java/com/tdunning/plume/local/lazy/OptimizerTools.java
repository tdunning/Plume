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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Lists;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.local.lazy.op.DeferredOp;
import com.tdunning.plume.local.lazy.op.Flatten;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.MultipleParallelDo;
import com.tdunning.plume.local.lazy.op.OneToOneOp;

public class OptimizerTools {
  
  /**
   * This utility returns all the different MSCR blocks that can be created from this plan
   * 
   * @param <T>
   * @param output
   * @return
   */
  public static <T> Set<MSCR> getMSCRBlocks(PCollection<T> output) {
    Set<GroupByKey<?, ?>> groupBys = OptimizerTools.getAllGroupByKeys(output);
    Set<MSCR> mscrs = new HashSet<MSCR>();
    // For all found GroupByKey blocks
    for(GroupByKey<?, ?> groupBy: groupBys) {
      // Get its inputs
      Set<PCollection<?>> gByInputs = OptimizerTools.getInputsFor(groupBy);
      MSCR mscrToAdd = null;
      // Check if there is already one MSCR with at least one of this inputs
      for(MSCR mscr: mscrs) {
        for(PCollection<?> input: gByInputs) {
          if(mscr.hasInput(input)) {
            mscrToAdd = mscr;
            break;
          }
        }
      }
      if(mscrToAdd == null) { // otherwise create new MSCR
        mscrToAdd = new MSCR();
      }
      // Add all missing inputs to current MSCR
      for(PCollection<?> input: gByInputs) {
        if(!mscrToAdd.hasInput(input)) {
          mscrToAdd.addInput(input);
        }
      }
      mscrs.add(mscrToAdd); // Add if needed
    }
    return mscrs;
  }
  
  /**
   * This utility goes down-top from a GroupByKey to find all distinct input PCollections that seed it.
   * This is usefull to create MSCR operations as in 4.3 of FlumeJava paper
   * 
   * @param seed
   * @return
   */
  public static Set<PCollection<?>> getInputsFor(GroupByKey<?, ?> seed) {
    Set<PCollection<?>> result = new HashSet<PCollection<?>>();
    Stack<LazyCollection<?>> toVisit = new Stack<LazyCollection<?>>();
    Set<LazyCollection<?>> visited = new HashSet<LazyCollection<?>>();
    toVisit.push((LazyCollection<?>)seed.getOrigin());
    while(!toVisit.isEmpty()) {
      LazyCollection<?> current = toVisit.pop();
      visited.add(current);
      if(current.isMaterialized()) { // condition for being an input. This may change.
        result.add(current);
        continue;
      }
      if(current.getDeferredOp() instanceof GroupByKey) { // stop recursion here - nested GBK are not allowed
        continue;
      }
      DeferredOp op = current.getDeferredOp();
      if(op instanceof Flatten) {
        for(PCollection<?> input: ((Flatten<?>)op).getOrigins()) {
          LazyCollection<?> in = (LazyCollection<?>)input;
          if(!visited.contains(in)) {
            toVisit.push(in);
          }
        }
        continue;
      }
      if(op instanceof OneToOneOp) {
        LazyCollection<?> input = (LazyCollection<?>)((OneToOneOp<?, ?>)op).getOrigin();
        if(!visited.contains(input)) {
          toVisit.push(input);
        }
        continue;
      }
      if(op instanceof MultipleParallelDo) {
        MultipleParallelDo<?> mPDo = (MultipleParallelDo<?>)op;
        LazyCollection<?> input = (LazyCollection<?>)mPDo.getOrigin();
        if(!visited.contains(input)) {
          toVisit.push(input);
        }
      }
    }
    return result;
  }
    
  /**
   * Navigate through all the tree and return the set of GroupByKey nodes found
   * 
   * @param output
   * @return
   */
  public static Set<GroupByKey<?, ?>> getAllGroupByKeys(PCollection<?> output) {
    Set<GroupByKey<?, ?>> groupByKeys = new HashSet<GroupByKey<?, ?>>();
    Stack<LazyCollection<?>> toVisit = new Stack<LazyCollection<?>>();
    Set<LazyCollection<?>> visited = new HashSet<LazyCollection<?>>();
    toVisit.push((LazyCollection<?>)output);
    /*
     * 1- Gather GroupByKey info
     */
    while(!toVisit.isEmpty()) {
      LazyCollection<?> current = toVisit.pop();
      visited.add(current);
      if(current.isMaterialized()) {
        continue;
      }
      DeferredOp op = current.getDeferredOp();
      if(op instanceof GroupByKey) {
        // Found GroupByKey
        groupByKeys.add((GroupByKey<?, ?>)op);
      } 
      // Add more nodes to visit
      List<DeferredOp> ops = Lists.newArrayList();
      if(current.getDownOps() != null) {
        current.getDownOps();
      }
      ops.add(op);
      for(DeferredOp o: ops) {
        if(o instanceof Flatten) {
          for(PCollection<?> input: ((Flatten<?>)o).getOrigins()) {
            LazyCollection<?> in = (LazyCollection<?>)input;
            if(!visited.contains(in)) {
              toVisit.push(in);
            }
          }
          continue;
        }
        if(o instanceof OneToOneOp) {
          LazyCollection<?> input = (LazyCollection<?>)((OneToOneOp<?, ?>)o).getOrigin();
          if(!visited.contains(input)) {
            toVisit.push(input);
          }
          continue;
        }
        if(o instanceof MultipleParallelDo) {
          MultipleParallelDo<?> mPDo = (MultipleParallelDo<?>)o;
          LazyCollection<?> input = (LazyCollection<?>)mPDo.getOrigin();
          if(!visited.contains(input)) {
            toVisit.push(input);
          }
          for(Map.Entry<?, ?> entry: mPDo.getDests().entrySet()) {
            LazyCollection<?> in = (LazyCollection<?>)entry.getKey();
            if(!visited.contains(in)) {
              toVisit.push(in);
            }
          }
        }
      }
    }
    return groupByKeys;
  }
}
