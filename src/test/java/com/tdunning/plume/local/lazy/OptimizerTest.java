package com.tdunning.plume.local.lazy;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.local.lazy.op.ParallelDo;

public class OptimizerTest {

  @Test
  /**
   * In this test we will apply (x + 1) o (x * 2) => (x + 1)*2
   */
  public void testParallelDoFusion() {
    DoFn<Integer, Integer> plusOne = new DoFn<Integer, Integer>() {
      @Override
      public void process(Integer v, EmitFn<Integer> emitter) {
        emitter.emit(v + 1);
      }
    };
    DoFn<Integer, Integer> timesTwo = new DoFn<Integer, Integer>() {
      @Override
      public void process(Integer v, EmitFn<Integer> emitter) {
        emitter.emit(v * 2);
      }
    };
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection<Integer> input  = plume.fromJava(Lists.newArrayList(1, 2, 3));
    PCollection<Integer> output = input.map(plusOne, null).map(timesTwo, null);
    // Execute and assert the result before optimizing
    executeAndAssert((LazyCollection<Integer>)output, new Integer[] { 4, 6, 8 });
    // Get an Optimizer
    Optimizer optimizer = new Optimizer();
    optimizer.joinParallelDos(output);
    // Check that optimizer did what it's supposed to do
    LazyCollection<Integer> lOutput = (LazyCollection<Integer>)output;
    @SuppressWarnings("unchecked")
    ParallelDo<Integer, Integer> newPDo = (ParallelDo<Integer, Integer>)lOutput.getDeferredOp();
    // Check that composed function does (x+1)*2
    newPDo.getFunction().process(5, new EmitFn<Integer>() {
      @Override
      public void emit(Integer v) {
        assertEquals(v.intValue(), 12); // (5+1)*2
      }
    });
    // Check that now output's parent is input
    assertEquals(newPDo.getOrigin(), input);
    LazyCollection<Integer> orig = (LazyCollection<Integer>)input;
    assertEquals(orig.getDownOps().size(), 1); // Check that downOps hasn't grow 
    // Execute and assert the result after optimizing
    executeAndAssert(lOutput, new Integer[] { 4, 6, 8 });
  }
  
  void executeAndAssert(LazyCollection<Integer> output, Integer[] expectedResult) {
    // Get an executor
    Executor executor = new Executor();
    // Execute current plan
    Iterable<Integer> result = executor.execute(output);
    List<Integer> l = Lists.newArrayList(result);
    Collections.sort(l);
    for(int i = 0; i < expectedResult.length; i++) {
      assertEquals(l.get(i).intValue(), expectedResult[i].intValue());    
    }
  }
}
