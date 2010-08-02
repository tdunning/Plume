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

package com.tdunning.plume;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tdunning.plume.local.eager.LocalPlume;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class FlattenTest {
  @Test
  public void flatten() {
    Set<Integer> x1 = Sets.newHashSet();
    for (int i = 0; i < 10; i++) {
      x1.add(i);
    }

    List<Integer> x2 = Lists.newArrayList();
    for (int i = 5; i < 15; i++) {
      x2.add(i);
    }

    Plume p = new LocalPlume();
    PCollection<Integer> x3 = p.flatten(p.fromJava(x1), p.fromJava(x2));
    PTable<Integer, Integer> x4 = x3.count();

    Map<Integer, Integer> r = Maps.newHashMap();
    for (Pair<Integer, Integer> pair : x4) {
      r.put(pair.getKey(), pair.getValue());
    }
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(new Integer(1), r.get(i));
    }
    for (int i = 5; i < 10; i++) {
      Assert.assertEquals(new Integer(2), r.get(i));
    }
    for (int i = 10; i < 15; i++) {
      Assert.assertEquals(new Integer(1), r.get(i));
    }

    PTable<Integer, String> x5 = x4.map(new DoFn<Pair<Integer, Integer>, Pair<Integer, String>>() {
      @Override
      public void process(Pair<Integer, Integer> v, EmitFn<Pair<Integer, String>> emitter) {
        emitter.emit(new Pair<Integer, String>(v.getKey(), v.getValue().toString()));
      }
    }, p.tableOf(Integer.class, String.class));
    for (Pair<Integer, String> pair : x5) {
      Assert.assertEquals(r.get(pair.getKey()).toString(), pair.getValue());
    }

    PCollection<String> x6 = x4.map(new DoFn<Pair<Integer, Integer>, String>() {
      @Override
      public void process(Pair<Integer, Integer> v, EmitFn<String> emitter) {
        emitter.emit(v.getValue().toString());
      }
    }, p.collectionOf(String.class));
    Map<String, Integer> r2 = Maps.newHashMap();
    for (Pair<String, Integer> v : x6.count()) {
      r2.put(v.getKey(), v.getValue());
    }
  }
}
