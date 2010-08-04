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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.types.IntegerType;
import com.tdunning.plume.types.PTableType;

/**
 * These basic tests can be used to assert that the Executor behaves correctly for all basic operations 
 */
public class BasicExecutorTests extends BaseTestClass {

  /**
   * This test runs a chain of two "ParallelDo" operations: (x+1), (x*2)
   */
  @Test
  public void testNestedMap() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection<Integer> input  = plume.fromJava(Lists.newArrayList(1, 2, 3));
    PCollection<Integer> output = input.map(plusOne, null).map(timesTwo, null);
    executeAndAssert((LazyCollection<Integer>)output, new Integer[] { 4, 6, 8 });
  }
  
  /**
   * Deferred execution of ((1,2,3)+(4,5,6)) => x+1 
   */
  @Test
  public void testMapAndFlatten() {
    List<Integer> l1 = Lists.newArrayList(1, 2, 3);
    List<Integer> l2 = Lists.newArrayList(4, 5, 6);
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    PCollection<Integer> output = plume.flatten(plume.fromJava(l1), plume.fromJava(l2)).map(plusOne, null);
    executeAndAssert((LazyCollection<Integer>)output, new Integer[] { 2, 3, 4, 5, 6, 7 });
  }
  
  /**
   * Try one deferred flatten (1,2,3)+(4,5,6)
   */
  @Test
  public void testSimpleFlatten() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    List<Integer> l1 = Lists.newArrayList(1, 2, 3);
    List<Integer> l2 = Lists.newArrayList(4, 5, 6);
    PCollection<Integer> output = plume.flatten(plume.fromJava(l1), plume.fromJava(l2));
    executeAndAssert((LazyCollection<Integer>)output, new Integer[] { 1, 2, 3, 4, 5, 6 });
  }
  
  /**
   * Try one nested deferred flatten (7,8,9)+((1,2,3)+(4,5,6))
   */
  @Test
  public void testNestedFlatten() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    List<Integer> l1 = Lists.newArrayList(1, 2, 3);
    List<Integer> l2 = Lists.newArrayList(4, 5, 6);
    List<Integer> l3 = Lists.newArrayList(7, 8, 9);
    PCollection<Integer> output =
      plume.flatten(plume.fromJava(l3), plume.flatten(plume.fromJava(l1), plume.fromJava(l2)));
    executeAndAssert((LazyCollection<Integer>)output, new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
  }
  
  /**
   * Try one group by from table (1,2),(1,3),(2,4),(2,6),(3,6),(3,9)
   */
  @Test
  public void testGroupByKey() {
    DoFn<Integer, Pair<Integer, Integer>> fn = new DoFn<Integer, Pair<Integer, Integer>>() {
      @Override
      public void process(Integer v, EmitFn<Pair<Integer, Integer>> emitter) {
        emitter.emit(Pair.create(v, v * 2));
        emitter.emit(Pair.create(v, v * 3));
      }
    };
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    List<Integer> l1 = Lists.newArrayList(1, 2, 3);
    PTable<Integer, Iterable<Integer>> output = plume
      .fromJava(l1).map(fn, new PTableType(new IntegerType(), new IntegerType()))
      .groupByKey();
    LazyTable<Integer, Iterable<Integer>> lOutput = (LazyTable<Integer, Iterable<Integer>>)output;
    // Get an executor
    Executor executor = new Executor();
    Iterable<Pair<Integer, Iterable<Integer>>> executedOutput = executor.execute(lOutput);
    List<Pair<Integer, Iterable<Integer>>> outputList = Lists.newArrayList(executedOutput);
    Collections.sort(outputList, new Comparator<Pair<Integer, ?>>() {
      @Override
      public int compare(Pair<Integer, ?> arg0, Pair<Integer, ?> arg1) {
        return arg0.getKey().compareTo(arg1.getKey());
      }
    });
    assertEquals(outputList.get(0).getKey().intValue(), 1);
    assertEquals(outputList.get(1).getKey().intValue(), 2);
    assertEquals(outputList.get(2).getKey().intValue(), 3);
    List<Integer> lR1 = Lists.newArrayList(outputList.get(0).getValue());
    List<Integer> lR2 = Lists.newArrayList(outputList.get(1).getValue());
    List<Integer> lR3 = Lists.newArrayList(outputList.get(2).getValue());
    Collections.sort(lR1);
    Collections.sort(lR2);
    Collections.sort(lR3);
    assertEquals(lR1.get(0).intValue(), 2);
    assertEquals(lR1.get(1).intValue(), 3);
    assertEquals(lR2.get(0).intValue(), 4);
    assertEquals(lR2.get(1).intValue(), 6);
    assertEquals(lR3.get(0).intValue(), 6);
    assertEquals(lR3.get(1).intValue(), 9);    
  }
  
  /**
   * Group by and combine adding all values from table (1,2),(1,3),(2,4),(2,6),(3,6),(3,9)
   * Should raise result (1,(2+3)),(2,(4+6)),(3,(6+9)) = (1,5),(2,10),(3,15)
   */
  @Test
  public void testCombine() {
    DoFn<Integer, Pair<Integer, Integer>> fn = new DoFn<Integer, Pair<Integer, Integer>>() {
      @Override
      public void process(Integer v, EmitFn<Pair<Integer, Integer>> emitter) {
        emitter.emit(Pair.create(v, v * 2));
        emitter.emit(Pair.create(v, v * 3));
      }
    };
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    List<Integer> l1 = Lists.newArrayList(1, 2, 3);
    PTable<Integer, Integer> output = plume
      .fromJava(l1).map(fn, new PTableType(new IntegerType(), new IntegerType()))
      .groupByKey()
      .combine(new CombinerFn<Integer>() {
        @Override
        public Integer combine(Iterable<Integer> stuff) {
          Integer result = 0;
          for(Integer i: stuff) {
            result += i;
          }
          return result;
        }
      });
    // Get an executor
    Executor executor = new Executor();
    LazyTable<Integer, Integer> lOutput = (LazyTable<Integer, Integer>)output;
    Iterable<Pair<Integer, Integer>> executedOutput = executor.execute(lOutput);
    List<Pair<Integer, Integer>> outputList = Lists.newArrayList(executedOutput);
    Collections.sort(outputList, new Comparator<Pair<Integer, ?>>() {
      @Override
      public int compare(Pair<Integer, ?> arg0, Pair<Integer, ?> arg1) {
        return arg0.getKey().compareTo(arg1.getKey());
      }
    });
    assertEquals(outputList.get(0).getKey().intValue(), 1);
    assertEquals(outputList.get(0).getValue().intValue(), 5);
    assertEquals(outputList.get(1).getKey().intValue(), 2);
    assertEquals(outputList.get(1).getValue().intValue(), 10);
    assertEquals(outputList.get(2).getKey().intValue(), 3);
    assertEquals(outputList.get(2).getValue().intValue(), 15);
  }
}
