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

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.plume.PCollection;
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
}
