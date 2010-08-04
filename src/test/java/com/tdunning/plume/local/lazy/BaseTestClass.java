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

import java.util.Collections;
import java.util.List;

import org.junit.Before;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;

/**
 * Contains some utility methods and variables for local.lazy testing
 */
public class BaseTestClass {

  DoFn<Integer, Integer> plusOne;
  DoFn<Integer, Integer> timesTwo;
  
  @Before
  public void initFns() {
    plusOne = new DoFn<Integer, Integer>() {
      @Override
      public void process(Integer v, EmitFn<Integer> emitter) {
        emitter.emit(v + 1);
      }
    };
    timesTwo = new DoFn<Integer, Integer>() {
      @Override
      public void process(Integer v, EmitFn<Integer> emitter) {
        emitter.emit(v * 2);
      }
    };
  }
  
  static void executeAndAssert(LazyCollection<Integer> output, Integer[] expectedResult) {
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
