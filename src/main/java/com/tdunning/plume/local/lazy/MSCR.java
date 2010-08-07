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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.op.CombineValues;
import com.tdunning.plume.local.lazy.op.Flatten;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.ParallelDo;

/**
 * The MSCR abstraction as in FlumeJava paper
 * 
 **/
public class MSCR {

  public static class OutputChannel<K, V, T> {

    Flatten<Pair<K, V>> flatten = null;
    GroupByKey<K, V> shuffle = null;
    CombineValues<K, V> combiner = null;
    ParallelDo<Pair<K, V>, T> reducer = null;
    
    public OutputChannel(GroupByKey<K, V> shuffle) {
      this.shuffle = shuffle;
    }
  }

  Set<PCollection<?>> inputs = new HashSet<PCollection<?>>();
  Map<GroupByKey<?, ?>, OutputChannel<?, ?, ?>> outputChannels = 
    new HashMap<GroupByKey<?, ?>, OutputChannel<?, ?, ?>>();
  Set<PCollection<?>> bypassChannels = new HashSet<PCollection<?>>();
 
  public Set<PCollection<?>> getInputs() {
    return inputs;
  }

  public <T> void addInput(PCollection<T> input) {
    inputs.add(input);
  }
  
  public <T> boolean hasInput(PCollection<T> input) {
    return inputs.contains(input);
  }
  
  public <K, V> boolean hasOutputChannel(GroupByKey<K, V> groupByChannel) {
    return outputChannels.containsKey(groupByChannel);
  }
  
  public <T> boolean hasOutputChannel(PCollection<T> byPassChannel) {
    return bypassChannels.contains(byPassChannel);
  }
  
  public <T> void addOutputChannel(PCollection<T> byPassChannel) {
    bypassChannels.add(byPassChannel);
  }
  
  public <K, V, T> void addOutputChannel(OutputChannel<K, V, T> outputChannel) {
    if(outputChannel.shuffle == null) {
      throw new IllegalArgumentException("Output Channel with no Shuffle");
    }
    outputChannels.put(outputChannel.shuffle, outputChannel);
  }
}
