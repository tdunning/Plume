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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.local.lazy.op.DeferredOp;
import com.tdunning.plume.local.lazy.op.Flatten;
import com.tdunning.plume.local.lazy.op.MultipleParallelDo;
import com.tdunning.plume.local.lazy.op.ParallelDo;

/**
 * These basic tests can be used to assert that the {@link Optimizer} behaves correctly for all basic operations 
 */
public class BasicOptimizerTest extends BaseTestClass {
  
  @SuppressWarnings("unchecked")
  @Test
  public void testParallelDoSiblingFusion() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection<Integer> input = plume.fromJava(Lists.newArrayList(1, 2, 3));
    PCollection<Integer> output1 = input.map(plusOne, null);
    PCollection<Integer> output2 = input.map(timesTwo, null);
    LazyCollection<Integer> lInput = (LazyCollection<Integer>)input;
    LazyCollection<Integer> lOutput1 = (LazyCollection<Integer>)output1;
    LazyCollection<Integer> lOutput2 = (LazyCollection<Integer>)output2;
    assertEquals(lInput.downOps.size(), 2);
    // Execute and assert the result before optimizing
    executeAndAssert(lOutput1, new Integer[] { 2, 3, 4 });
    executeAndAssert(lOutput2, new Integer[] { 2, 4, 6 });
    // Get an Optimizer
    Optimizer optimizer = new Optimizer();
    optimizer.fuseSiblingParallelDos(output1); // one output is enough to fuse both because they share the parent
    // Check that input child ops has shrinked to 1
    assertEquals(lInput.downOps.size(), 1);
    DeferredOp op = lInput.downOps.get(0);
    // Check that there is only one op pointing to both outputs
    assertEquals(op, lOutput1.deferredOp);
    assertEquals(op, lOutput2.deferredOp);
    assertTrue(op instanceof MultipleParallelDo);
    MultipleParallelDo<Integer> mPDo = (MultipleParallelDo<Integer>)op;
    Map<PCollection<?>, DoFn<Integer, ?>> mapOfPDos = mPDo.getDests();
    // Check that the map of functions in MultipleParallelDo is correct
    assertEquals(mapOfPDos.get(output1), plusOne);
    assertEquals(mapOfPDos.get(output2), timesTwo);
    // Execute and assert the result afer optimizing
    executeAndAssert(lOutput1, new Integer[] { 2, 3, 4 });
    executeAndAssert(lOutput2, new Integer[] { 2, 4, 6 });
  }
  
  /**
   * In this test we will apply one fuseParallelDo (x + 1) o (x * 2) => (x + 1)*2
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testParallelDoFusion() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection<Integer> input  = plume.fromJava(Lists.newArrayList(1, 2, 3));
    PCollection<Integer> output = input.map(plusOne, null).map(timesTwo, null);
    // Execute and assert the result before optimizing
    executeAndAssert((LazyCollection<Integer>)output, new Integer[] { 4, 6, 8 });
    LazyCollection<Integer> lOutput = (LazyCollection<Integer>)output;
    ParallelDo<Integer, Integer> oldPDo = (ParallelDo<Integer, Integer>)lOutput.getDeferredOp();
    LazyCollection<Integer> intermediateCol = (LazyCollection<Integer>)oldPDo.getOrigin();
    assertEquals(intermediateCol.getDownOps().size(), 1);
    assertEquals(intermediateCol.getDownOps().get(0), oldPDo);
    // Get an Optimizer
    Optimizer optimizer = new Optimizer();
    optimizer.fuseParallelDos(output);
    // Check that optimizer did what it's supposed to do
    ParallelDo<Integer, Integer> newPDo = (ParallelDo<Integer, Integer>)lOutput.getDeferredOp();
    assertFalse(newPDo == oldPDo);
    assertEquals(intermediateCol.getDownOps().size(), 0);
    // Check that composed function does (x+1)*2
    newPDo.getFunction().process(5, new EmitFn<Integer>() {
      @Override
      public void emit(Integer v) {
        assertEquals(v.intValue(), 12); // (5+1)*2
      }
    });
    // Check that now output's parent is input
    assertEquals(newPDo.getOrigin(), input);
    // Execute and assert the result after optimizing
    executeAndAssert(lOutput, new Integer[] { 4, 6, 8 });
  }
  
  /**
   * This test has two inputs, one flatten and then one ParallelDo.
   * After sinking flattens, the tree should be as: two inputs, one ParallelDo after each input and one final Flatten.
   */
  @Test
  public void testSinkFlattens() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection<Integer> input1  = plume.fromJava(Lists.newArrayList(1, 2, 3));
    PCollection<Integer> input2  = plume.fromJava(Lists.newArrayList(4, 5, 6));
    PCollection<Integer> output = plume.flatten(input1, input2).map(plusOne, null);
    LazyCollection<Integer> lOutput = (LazyCollection<Integer>)output;
    assertTrue(lOutput.getDeferredOp() instanceof ParallelDo);
    // Execute and assert the result before optimizing
    executeAndAssert((LazyCollection<Integer>)output, new Integer[] { 2, 3, 4, 5, 6, 7 });
    // Get an Optimizer
    Optimizer optimizer = new Optimizer();
    optimizer.sinkFlattens(output);
    // Execute and assert the result after optimizing
    executeAndAssert((LazyCollection<Integer>)output, new Integer[] { 2, 3, 4, 5, 6, 7 });    
    // Check that optimizer did what it's supposed to do
    assertTrue(lOutput.getDeferredOp() instanceof Flatten);
    Flatten flatten = (Flatten)lOutput.getDeferredOp();
    assertEquals(flatten.getOrigins().size(), 2);
    for(int i = 0; i < 2; i++) {
      LazyCollection<Integer> origin = (LazyCollection<Integer>) flatten.getOrigins().get(i);
      ParallelDo newPDo = (ParallelDo)origin.getDeferredOp();
      assertEquals(newPDo.getFunction(), plusOne);
      assertTrue(newPDo.getOrigin() == input1 || newPDo.getOrigin() == input2);
    }
  }
}
