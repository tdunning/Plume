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
import java.util.List;

import com.tdunning.plume.PCollection;

/**
 * The purpose of this class is to encapsulate Plume workflows so that they can be instantiated 
 * in Mappers and Reducers.   
 */
public abstract class PlumeWorkflow {
  
  List<PCollection> inputs;
  List<PCollection> outputs;
  
  public abstract void build();

  public PlumeWorkflow() {
    init();
  }
  
  protected void init() {
    inputs = new ArrayList<PCollection>();
    outputs = new ArrayList<PCollection>();
  }
  
  protected void addOutput(PCollection collection) {
    outputs.add(collection);
  }
  
  protected void addInput(PCollection collection) {
    inputs.add(collection);
  }

  public List<PCollection> getInputs() {
    return inputs;
  }
  
  public List<PCollection> getOutputs() {
    return outputs;
  }
}
