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
import java.util.Iterator;
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
    // Get all GroupByKeys from the tree
    List<DeferredOp> groupBys = OptimizerTools.getAll(outputs, GroupByKey.class);
    int mscrId = 1;
    Set<MSCR> mscrs = new HashSet<MSCR>();
    // For all found GroupByKey blocks
    for(DeferredOp gBK: groupBys) {
      GroupByKey groupBy = (GroupByKey<?,?>)gBK;
      // Gather all information needed for MSCR from this GBK
      Set<PCollection<?>> inputs = new HashSet<PCollection<?>>();
      Set<GroupByKey<?, ?>> outputChannels = new HashSet<GroupByKey<?, ?>>();
      Set<Flatten<?>> unGroupedOutputChannels = new HashSet<Flatten<?>>();
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
          // Check for bypass channels & output channels with no group-by
          for(Map.Entry entry: mPDo.getDests().entrySet()) {
            LazyCollection coll = (LazyCollection)entry.getKey();
            if(coll.getDownOps() == null || coll.getDownOps().size() == 0) {
              bypassChannels.add(coll); // leaf node
            } else if(coll.getDownOps().get(0) instanceof MultipleParallelDo) {
              bypassChannels.add(coll);
            /*
             * Case of an output channel that Flattens with no Group By
             */
            } else if(coll.getDownOps().get(0) instanceof Flatten) {
              Flatten<?> thisFlatten = (Flatten<?>)coll.getDownOps().get(0);
              LazyCollection ldest = (LazyCollection)thisFlatten.getDest();
              if(ldest.getDownOps() == null || ldest.getDownOps().size() == 0 ||
                  ldest.getDownOps().get(0) instanceof MultipleParallelDo) {
                unGroupedOutputChannels.add(thisFlatten);
                // Add the rest of this flatten's origins to the stack in order to possibly discover more output channels
                for(PCollection<?> col: thisFlatten.getOrigins()) {
                  if(!visited.contains(col)) {
                    toVisit.push((LazyCollection<?>)col);
                  }
                }
              }
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
      // Add all missing bypass outputs to current MSCR
      for(PCollection<?> col: bypassChannels) {
        if(!mscrToAdd.hasOutputChannel(col)) {
          // Create new by-pass channel
          MSCR.OutputChannel oC = new MSCR.OutputChannel(col);
          mscrToAdd.addOutputChannel(oC);
        }
      }
      // Add all missing flatten-with-no-groupby outputs to current MSCR
      for(Flatten flatten: unGroupedOutputChannels) {
        if(!mscrToAdd.hasOutputChannel(flatten.getDest())) {
          // Create new channel with flatten and nothing else
          MSCR.OutputChannel oC = new MSCR.OutputChannel(flatten.getDest());
          oC.output = flatten.getDest();
          oC.flatten = flatten;
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
              oC.output = oC.reducer.getDest();
            }
          }
          mscrToAdd.addOutputChannel(oC);
        }
      }
      mscrs.add(mscrToAdd); // Add if needed
    }
    return addRemainingTrivialMSCRs(outputs, mscrId, mscrs);
  }
  
  /**
   * This utility returns all the MSCRs that are not related to a GroupByKey - 
   *  the remaining trivial cases as described in FlumeJava paper
   *  
   *  These cases will be either:
   *  - Flattens that are followed by either a)MultipleParallelDo or b)nothing
   *  
   *    (These ones can have correlated inputs and be parallelized just like the ones with GroupByKey)
   *    
   *  - The trivial Input->ParalleDo|MultipleParalleDo->Output case
   *  
   * @param outputs
   * @return
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  static Set<MSCR> addRemainingTrivialMSCRs(List<PCollection> outputs, int currentMscrId, Set<MSCR> currentMSCRs) {
    // Get all Flatten from the tree
    List<DeferredOp> flattens = OptimizerTools.getAll(outputs, Flatten.class);
    Set<MSCR> mscrs = new HashSet<MSCR>();
    mscrs.addAll(currentMSCRs);
    Iterator<DeferredOp> it = flattens.iterator();
    mainLoop: while(it.hasNext()) {
      Flatten<?> flatten = (Flatten<?>)it.next();
      // Process only remaining flattens that are not in any other MSCR
      for(MSCR mscr: mscrs) {
        for(Map.Entry<PCollection<?>, MSCR.OutputChannel<?, ?, ?>> entry: mscr.getOutputChannels().entrySet()) {
          if(entry.getValue().flatten != null && entry.getValue().flatten == flatten) {
            continue mainLoop; // skip this flatten
          }
        }
      }
      //
      MSCR mscr = new MSCR(currentMscrId);
      currentMscrId++;
      // add output channel
      MSCR.OutputChannel oC = new MSCR.OutputChannel(flatten.getDest());
      oC.output = flatten.getDest();
      oC.flatten = flatten;
      mscr.addOutputChannel(oC);
      // add inputs
      for(PCollection coll: flatten.getOrigins()) {
        LazyCollection lCol = (LazyCollection)coll;
        if(lCol.isMaterialized()) {
          mscr.addInput(coll);
        } else if(lCol.deferredOp instanceof ParallelDo) {
          ParallelDo pDo = (ParallelDo)lCol.deferredOp;
          if(((LazyCollection)pDo.getOrigin()).isMaterialized()) {
            mscr.addInput(pDo.getOrigin());
          } else if(pDo instanceof MultipleParallelDo) {
            mscr.addInput(pDo.getOrigin());
          } else {
            mscr.addInput(coll);
          }
        } else {
          mscr.addInput(coll);
        }
      }
      mscrs.add(mscr);
    }
    return mscrs;
  }
  
  /**
   * This utility navigates through all the tree and return the set of DeferredOp nodes found given the provided Class
   */
  static List<DeferredOp> getAll(List<PCollection> outputs, Class<? extends DeferredOp> getClass) {
    List<DeferredOp> ops = new ArrayList<DeferredOp>();
    for(PCollection<?> output: outputs) {
      List<DeferredOp> partialGroupBys = getAll(output, getClass);
      for(DeferredOp op: partialGroupBys) {
        if(!ops.contains(op)) {
          ops.add(op);
        }
      }
    }
    return ops;
  }

  static List<DeferredOp> getAll(PCollection<?> output, Class<? extends DeferredOp> getClass) {
    List<DeferredOp> retOps = new ArrayList<DeferredOp>();
    Stack<LazyCollection<?>> toVisit = new Stack<LazyCollection<?>>();
    Set<LazyCollection<?>> visited = new HashSet<LazyCollection<?>>();
    toVisit.push((LazyCollection<?>)output);
    while(!toVisit.isEmpty()) {
      LazyCollection<?> current = toVisit.pop();
      visited.add(current);
      if(current.isMaterialized()) {
        continue;
      }
      DeferredOp op = current.getDeferredOp();
      if(op.getClass().equals(getClass)) {
        // Found 
        if(!retOps.contains(op)) {
          retOps.add(op);
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
    return retOps;
  }
}
