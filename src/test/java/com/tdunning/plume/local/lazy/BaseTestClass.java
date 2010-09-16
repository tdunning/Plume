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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.Pair;
import com.tdunning.plume.types.IntegerType;
import com.tdunning.plume.types.PTableType;

/**
 * Contains some utility methods and variables for local.lazy testing
 */
public class BaseTestClass {

  final static DoFn<Integer, Integer> plusOne = new DoFn<Integer, Integer>() {
    @Override
    public void process(Integer v, EmitFn<Integer> emitter) {
      emitter.emit(v + 1);
    }
  };
  
  final static DoFn<Integer, Integer> timesTwo = new DoFn<Integer, Integer>() {
    @Override
    public void process(Integer v, EmitFn<Integer> emitter) {
      emitter.emit(v * 2);
    }
  };
  
  final static DoFn<Integer, Pair<Integer, Integer>> plusTwoPlusThree = new DoFn<Integer, Pair<Integer, Integer>>() {
    @Override
    public void process(Integer v, EmitFn<Pair<Integer, Integer>> emitter) {
      emitter.emit(Pair.create(v, v * 2));
      emitter.emit(Pair.create(v, v * 3));
    }
  };
  
  final static DoFn identity = new DoFn() {
    @Override
    public void process(Object v, EmitFn emitter) {
      emitter.emit(v);
    }      
  };
  
  final static CombinerFn dummyCombiner = new CombinerFn<Integer>() {
    @Override
    public Integer combine(Iterable<Integer> stuff) {
      return 1;
    }
  };
  
  final static CombinerFn countCombiner = new CombinerFn<IntWritable>() {
    @Override
    public IntWritable combine(Iterable<IntWritable> stuff) {
      int c = 0;
      for(IntWritable i : stuff) {
        c += i.get();
      }
      return new IntWritable(c);
    }
  };
  
  final static DoFn countReduceToText = new DoFn() {
    @Override
    public void process(Object v, EmitFn emitter) {
      Pair p = (Pair)v;
      emitter.emit(Pair.create(p.getKey(), 
        new Text(""+countCombiner.combine((Iterable<IntWritable>)p.getValue()))));
    }
  };
  
  static void executeAndAssert(LazyCollection<Integer> output, Integer[] expectedResult) {
    // Get a local executor
    LocalExecutor executor = new LocalExecutor();
    // Execute current plan
    Iterable<Integer> result = executor.execute(output);
    List<Integer> l = Lists.newArrayList(result);
    Collections.sort(l);
    for(int i = 0; i < expectedResult.length; i++) {
      assertEquals(l.get(i).intValue(), expectedResult[i].intValue());    
    }
  }
}
