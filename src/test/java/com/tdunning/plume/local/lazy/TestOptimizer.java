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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;

/**
 * Work in progress: more testing is needed
 */
public class TestOptimizer extends BaseTestClass {

  /**
   * Test figure 4 of FlumeJava paper
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testFigure4() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection input1 = plume.fromJava(Lists.newArrayList(Pair.create(1, 1)));
    PCollection input2 = plume.fromJava(Lists.newArrayList(Pair.create(2, 2)));
    PCollection input3 = plume.fromJava(Lists.newArrayList(Pair.create(3, 3)));
    PCollection input4 = plume.fromJava(Lists.newArrayList(Pair.create(4, 4)));
    
    PCollection output1 = plume.flatten(intIntTable,
        input1.map(identity, intIntTable),   
        input2.map(identity, intIntTable)).groupByKey();
        
    PCollection output2 = plume.flatten(intIntTable,
        input2.map(identity, intIntTable),
        input3.map(identity, intIntTable),
        input4.map(identity, intIntTable)).groupByKey().combine(dummyCombiner).map(identity, null);
    
    PCollection output3 = plume.flatten(intIntTable,
        input4.map(identity, intIntTable)).groupByKey().map(identity, null);
    
    Optimizer optimizer = new Optimizer();
    ExecutionStep step = optimizer.optimize(
        Lists.newArrayList(input1, input2, input3, input4), 
        Lists.newArrayList(output1, output2, output3)
    );
    
    assertEquals(step.mscrSteps.size(), 1);
    assertEquals(step.nextStep, null);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testFigure5() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection input1 = plume.fromJava(Lists.newArrayList(Pair.create(1, 1)));
    PCollection input2 = plume.fromJava(Lists.newArrayList(Pair.create(2, 2)));
    PCollection input3 = plume.fromJava(Lists.newArrayList(Pair.create(3, 3)));
    PCollection input4 = plume.fromJava(Lists.newArrayList(Pair.create(4, 4)));

    PCollection partial1 = input1.map(identity, intIntTable);
    PCollection partial2 =
      plume.flatten(intIntTable,
          input2.map(identity, intIntTable),
          input3.map(identity, intIntTable).map(identity, null).map(identity, null));
    PCollection partial3 =
      input4.map(identity, intIntTable).groupByKey().combine(dummyCombiner).map(identity, null);
  
    PCollection output = plume.flatten(intIntTable, partial1, partial2, partial3).groupByKey().map(identity, null);
    
    Optimizer optimizer = new Optimizer();
    ExecutionStep step = optimizer.optimize(
        Lists.newArrayList(input1, input2, input3, input4), 
        Lists.newArrayList(output, partial1)
    );
    
    assertEquals(step.mscrSteps.size(), 1);
    assertNotNull(step.nextStep);
    assertEquals(step.nextStep.mscrSteps.size(), 1);
    assertNull(step.nextStep.nextStep);
  }
  
  @Test
  public void testEasyChain() {
    LazyPlume plume = new LazyPlume();
    /*
     * Two lists, empty mapper, reducer that emits first number
     * 
     * Input: (1,1),(1,2),(1,3) + (2,10),(2,20),(3,30) => (1, (1,2,3)),(2, (10,20)),(3,30) => (1,1),(2,10),(3,30)
     */
    List<Pair<Integer,Integer>> l1 = Lists.newArrayList(Pair.create(1,1),Pair.create(1,2),Pair.create(1,3));
    List<Pair<Integer,Integer>> l2 = Lists.newArrayList(Pair.create(2,10),Pair.create(2,20),Pair.create(3,30));
    PTable<Integer,Integer> i1 = plume.fromJava(l1, intIntTable);
    PTable<Integer,Integer> i2 = plume.fromJava(l2, intIntTable);
    PTable<Integer,Integer> o = plume.flatten(intIntTable, i1, i2).groupByKey().map(new DoFn<Pair<Integer, Iterable<Integer>>, Pair<Integer, Integer>>() {
      @Override
      public void process(Pair<Integer, Iterable<Integer>> v,
          EmitFn<Pair<Integer, Integer>> emitter) {
        emitter.emit(Pair.create(v.getKey(), v.getValue().iterator().next()));
      }
    }, intIntTable);
    
    Executor executor = new Executor();
    Iterable<Pair<Integer,Integer>> result = executor.execute((LazyTable<Integer, Integer>)o);
    Iterator<Pair<Integer,Integer>> it = result.iterator();
    Pair<Integer,Integer> next = it.next();
    // assert the expected result without assuming pairs are ordered
    assertTrue(next.getKey() == 1 && next.getValue() == 1 || next.getKey() == 2 && next.getValue() == 10 || next.getKey() == 3 && next.getValue() == 30);
    next = it.next();
    assertTrue(next.getKey() == 1 && next.getValue() == 1 || next.getKey() == 2 && next.getValue() == 10 || next.getKey() == 3 && next.getValue() == 30);
    next = it.next();
    assertTrue(next.getKey() == 1 && next.getValue() == 1 || next.getKey() == 2 && next.getValue() == 10 || next.getKey() == 3 && next.getValue() == 30);
    
    Optimizer optimizer = new Optimizer();
    List<PCollection> outputs = new ArrayList<PCollection>();
    List<PCollection> inputs = new ArrayList<PCollection>();
    inputs.add(i1); inputs.add(i2);
    outputs.add(o);
    ExecutionStep step = optimizer.optimize(inputs, outputs);
    assertTrue(step.getMscrSteps().size() == 1);

    MSCR toExecute = step.getMscrSteps().iterator().next();
    assertEquals(toExecute.inputs.size(), 2);
    assertEquals(toExecute.outputChannels.size(), 1);
  }
}