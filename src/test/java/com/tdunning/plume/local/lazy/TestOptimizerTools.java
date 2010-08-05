package com.tdunning.plume.local.lazy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.ParallelDo;

public class TestOptimizerTools extends BaseTestClass {

  @Test
  @SuppressWarnings({"unchecked"})
  public void testGroupByKeys() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection<Integer> input1 = plume.fromJava(Lists.newArrayList(1, 2, 3));
    PCollection<Integer> input2 = plume.fromJava(Lists.newArrayList(4, 5, 6));
    PCollection<Integer> output =
    plume.flatten(
        input1.map(plusTwoPlusThree, intIntTable)
        .groupByKey(), 
        input2.map(plusTwoPlusThree, intIntTable)
        .groupByKey())
        .map(new DoFn<Pair<Integer, Iterable<Integer>>, Integer>() {
      @Override
      public void process(Pair<Integer, Iterable<Integer>> v,
          EmitFn<Integer> emitter) {
        emitter.emit(1);
      }
    }, null);
    Set<GroupByKey<?, ?>> groupBys = OptimizerTools.getAllGroupByKeys(output);
    assertEquals(groupBys.size(), 2);
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testGroupByKeysAndGetInputsFor() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection<Integer> input1 = plume.fromJava(Lists.newArrayList(1, 2, 3));
    PCollection<Integer> input2 = plume.fromJava(Lists.newArrayList(4, 5, 6));
    PCollection<Integer> input3 = plume.fromJava(Lists.newArrayList(7, 8, 9));
    // One inner group by key
    PCollection<?> output =
      plume.flatten(
          input1.map(plusTwoPlusThree, intIntTable),
          input2.map(plusTwoPlusThree, intIntTable),
          input3.map(plusTwoPlusThree, intIntTable).groupByKey().combine(dummyCombiner)
        );
    // Second group by key
    output = output.map(identity, intIntTable).groupByKey();
    Set<GroupByKey<?, ?>> groupBys = OptimizerTools.getAllGroupByKeys(output);
    assertEquals(groupBys.size(), 2);
    Iterator<GroupByKey<?, ?>> iterator = groupBys.iterator();
    for(int i = 0; i < 2; i++) {
      GroupByKey<?, ?> groupByKey = iterator.next();
      ParallelDo<?, ?> op = (ParallelDo<?, ?>)groupByKey.getOrigin().getDeferredOp();
      assertTrue(op.getFunction() == identity || op.getFunction() == plusTwoPlusThree);
      Set<PCollection<?>> inputs = OptimizerTools.getInputsFor(groupByKey);
      if(op.getFunction() == identity) {
        assertEquals(inputs.size(), 2); // second group by
      } else {
        assertEquals(inputs.size(), 1); // inner group by
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testMSCRBlocks() {
    // Get Plume runtime
    LazyPlume plume = new LazyPlume();
    // Create simple data 
    PCollection<Integer> input1 = plume.fromJava(Lists.newArrayList(1, 2, 3));
    PCollection<Integer> input2 = plume.fromJava(Lists.newArrayList(4, 5, 6));
    PCollection<Integer> input3 = plume.fromJava(Lists.newArrayList(7, 8, 9));
    // One inner group by key
    PCollection<?> output =
      plume.flatten(
          input1.map(plusTwoPlusThree, intIntTable),
          input2.map(plusTwoPlusThree, intIntTable),
          input3.map(plusTwoPlusThree, intIntTable).groupByKey().combine(dummyCombiner)
        )
        .map(identity, intIntTable)
        .groupByKey();
    
    Set<MSCR> mscrBlocks = OptimizerTools.getMSCRBlocks(output);
    assertEquals(mscrBlocks.size(), 2);
    Iterator<MSCR> iterator = mscrBlocks.iterator();
    for(int i = 0; i < 2; i++) {
      MSCR mscr = iterator.next();
      if(mscr.hasInput(input1)) {
        assertTrue(mscr.hasInput(input2));
        assertEquals(mscr.inputs.size(), 2);
      } else if(mscr.hasInput(input3)) { 
        assertEquals(mscr.inputs.size(), 1);
      } 
    }
  }
}
