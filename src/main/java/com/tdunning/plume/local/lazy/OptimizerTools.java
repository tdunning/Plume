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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Lists;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.local.lazy.op.CombineValues;
import com.tdunning.plume.local.lazy.op.DeferredOp;
import com.tdunning.plume.local.lazy.op.Flatten;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.MultipleParallelDo;
import com.tdunning.plume.local.lazy.op.OneToOneOp;
import com.tdunning.plume.local.lazy.op.ParallelDo;

/**
 * This class is an extension to {@link Optimizer} that decouples some of its logic to make testing more easy.
 */
public class OptimizerTools {
  
  /**
   * This utility returns all the different MSCR blocks that can be created from this plan
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  static Set<MSCR> getMSCRBlocks(List<PCollection> outputs) {
    List<GroupByKey<?, ?>> groupBys = new ArrayList<GroupByKey<?, ?>>();
    for(PCollection<?> output: outputs) {
      List<GroupByKey<?, ?>> partialGroupBys = OptimizerTools.getAllGroupByKeys(output);
      for(GroupByKey<?, ?> gBK: partialGroupBys) {
        if(!groupBys.contains(gBK)) {
          groupBys.add(gBK);
        }
      }
    }
    int mscrId = 1;
    Set<MSCR> mscrs = new HashSet<MSCR>();
    // For all found GroupByKey blocks
    for(GroupByKey<?, ?> groupBy: groupBys) {
      // Gather all information needed for MSCR from this GBK
      Set<PCollection<?>> inputs = new HashSet<PCollection<?>>();
      Set<GroupByKey<?, ?>> outputChannels = new HashSet<GroupByKey<?, ?>>();
      Set<PCollection<?>> bypassChannels = new HashSet<PCollection<?>>();
      Stack<LazyCollection<?>> toVisit = new Stack<LazyCollection<?>>();
      Set<LazyCollection<?>> visited = new HashSet<LazyCollection<?>>();
      LazyCollection<?> origin = (LazyCollection<?>)groupBy.getOrigin();
      toVisit.push(origin);
      outputChannels.add(groupBy);
      while(!toVisit.isEmpty()) {
        LazyCollection<?> current = toVisit.pop();
        visited.add(current);
        if(current.isMaterialized()) { // condition for being a materialized input. This may change.
          inputs.add(current);
          continue;
        }
        DeferredOp op = current.getDeferredOp();
        if(op instanceof MultipleParallelDo) { // second condition for being an input
          MultipleParallelDo<?> mPDo = (MultipleParallelDo)current.getDeferredOp();
          if(((LazyCollection<?>)mPDo.getOrigin()).isMaterialized()) {
            inputs.add(mPDo.getOrigin()); // will be done in Mapper
          } else if(op instanceof ParallelDo) {
            inputs.add(current); // will be done in Reducer
          } else {
            inputs.add(mPDo.getOrigin()); // will be done in Mapper
          }
          // Check for bypass channels
          for(Map.Entry entry: mPDo.getDests().entrySet()) {
            LazyCollection coll = (LazyCollection)entry.getKey();
            if(coll.getDownOps() == null || coll.getDownOps().size() == 0) {
              bypassChannels.add(coll);
            }
          }
          continue;
        }
        if(op instanceof GroupByKey) { // third condition for being an input - rare case when one GBK follows another
          inputs.add(current);
          continue;
        }
        if(op instanceof Flatten) {
          Flatten<?> flatten = (Flatten<?>)op;
          for(PCollection<?> input: flatten.getOrigins()) {
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
      }
      MSCR mscrToAdd = null;
      // Check if there is already one MSCR with at least one of this inputs
      for(MSCR mscr: mscrs) {
        for(PCollection<?> input: inputs) {
          if(mscr.hasInput(input)) {
            mscrToAdd = mscr;
            break;
          }
        }
      }
      if(mscrToAdd == null) { // otherwise create new MSCR
        mscrToAdd = new MSCR(mscrId);
        mscrId++;
      }
      // Add all missing input channels to current MSCR
      for(PCollection<?> input: inputs) {
        if(!mscrToAdd.hasInput(input)) {
          mscrToAdd.addInput(input);
        }
      }
      // Add all missing ungroupped outputs to current MSCR
      for(PCollection<?> col: bypassChannels) {
        if(!mscrToAdd.hasOutputChannel(col)) {
          // Create new by-pass channel
          MSCR.OutputChannel oC = new MSCR.OutputChannel(col);
          mscrToAdd.addOutputChannel(oC);
        }
      }
      // Add all missing output channels to current MSCR
      for(GroupByKey groupByKey: outputChannels) {
        if(!mscrToAdd.hasOutputChannel(groupByKey.getOrigin())) {
          // Create new channel with group by key. It might have combiner and reducer as well.
          MSCR.OutputChannel oC = new MSCR.OutputChannel(groupByKey);
          oC.output = groupByKey.getDest();
          if(groupByKey.getOrigin().getDeferredOp() instanceof Flatten) {
            oC.flatten = (Flatten)groupByKey.getOrigin().getDeferredOp();
            oC.output = oC.flatten.getDest();
          }
          if(groupByKey.getDest().getDownOps() != null && groupByKey.getDest().getDownOps().size() == 1) {
            DeferredOp op = (DeferredOp)groupByKey.getDest().getDownOps().get(0);
            if(op instanceof CombineValues) {
              oC.combiner = (CombineValues)op;
              oC.output = oC.combiner.getDest();
              LazyCollection dest = (LazyCollection)oC.combiner.getDest();
              if(dest.getDownOps() != null && dest.getDownOps().size() == 1) {
                op = (DeferredOp)dest.getDownOps().get(0);
              }
            }
            if(op instanceof ParallelDo) {
              oC.reducer = (ParallelDo)op;
              // TODO what if it's multiple parallel do?
              oC.output = oC.reducer.getDest();
            }
          }
          mscrToAdd.addOutputChannel(oC);
        }
      }
      mscrs.add(mscrToAdd); // Add if needed
    }
    return mscrs;
  }
  
  /**
   * This utility navigates through all the tree and return the set of GroupByKey nodes found
   */
  static List<GroupByKey<?, ?>> getAllGroupByKeys(PCollection<?> output) {
    List<GroupByKey<?, ?>> groupByKeys = new ArrayList<GroupByKey<?, ?>>();
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
        GroupByKey<?, ?> gBK = (GroupByKey<?, ?>)op;
        if(!groupByKeys.contains(gBK)) {
          groupByKeys.add(gBK);
        }
      } 
      // Add more nodes to visit
      List<DeferredOp> ops = Lists.newArrayList();
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
        }
      }
    }
    return groupByKeys;
  }
}
