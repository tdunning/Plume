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

package com.tdunning.plume.local.mapReduce;

import com.google.common.base.Function;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.tdunning.plume.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MapReduceTest {
  @Test
  public void mapOnly() {
    MapReduceBuilder<Integer, Integer, Integer, Integer, Integer> x1 = new MapReduceBuilder<Integer, Integer, Integer, Integer, Integer>();
    MapReduceBuilder<Integer, Integer, Integer, Integer, Integer> x2 = x1.map(new Mapper<Integer, Integer, Integer, Integer>() {
      @Override
      public void map(Integer key, Integer value, Collector<Integer, Integer> out) {
        out.collect(key, value + 1);
      }
    });
    MapReduce<Integer, Integer, Integer, Integer, Integer> mr = x2.build();

    Random gen = new Random();

    List<Pair<Integer, Integer>> in = Lists.newArrayList(
            Pair.create(gen.nextInt(), 1),
            Pair.create(gen.nextInt(), 2),
            Pair.create(gen.nextInt(), 3),
            Pair.create(gen.nextInt(), 4)
    );

    Iterable<Pair<Integer, Integer>> out = mr.run(in);

    Set<Integer> r = Sets.newTreeSet(Iterables.transform(out, new Function<Pair<Integer, Integer>, Integer>() {
      @Override
      public Integer apply(Pair<Integer, Integer> x) {
        return x.getValue();
      }
    }));

    assertEquals(4, r.size());
    r.removeAll(Lists.newArrayList(2, 3, 4, 5));
    assertEquals(0, r.size());
  }

  @Test
  public void mapAndReduce() {
    List<Pair<Integer, String>> words = Lists.newArrayList();
    Multiset<String> ref = HashMultiset.create();

    int k = 0;
    Random gen = new Random();
    for (String letter : "abcdefghij".split("")) {
      // add 2^k of this letter
      for (int i = 0; i < (1 << k); i++) {
        words.add(Pair.create(gen.nextInt(), letter));
        ref.add(letter);
      }
      k++;
    }

    MapReduce<Integer, String, String, Integer, Integer> mr = new MapReduceBuilder<Integer, String, String, Integer, Integer>()
            .map(new Mapper<Integer, String, String, Integer>() {
              @Override
              public void map(Integer key, String value, Collector<String, Integer> out) {
                out.collect(value, 1);
              }
            })
            .reduce(new Reducer<String, Integer, Integer>() {
              @Override
              public void reduce(String key, Iterable<Integer> values, Collector<String, Integer> out) {
                int sum = 0;
                for (Integer value : values) {
                  sum += value;
                }
                out.collect(key, sum);
              }
            })
            .build();

    Iterable<Pair<String, Integer>> out = mr.run(words);
    for (Pair<String, Integer> pair : out) {
      assertEquals(ref.count(pair.getKey()), pair.getValue().intValue());
    }
  }

  @Test
  public void mapReduceAndCombine() {
    List<Pair<Integer, String>> words = Lists.newArrayList();
    Multiset<String> ref = HashMultiset.create();

    int k = 0;
    Random gen = new Random();
    for (String letter : "abcdefghij".split("")) {
      // add 2^k of this letter
      for (int i = 0; i < (1 << k); i++) {
        words.add(Pair.create(gen.nextInt(), letter));
        ref.add(letter);
      }
      k++;
    }

    Reducer<String, Integer, Integer> r = new Reducer<String, Integer, Integer>() {
      @Override
      public void reduce(String key, Iterable<Integer> values, Collector<String, Integer> out) {
        int sum = 0;
        for (Integer value : values) {
          sum += value;
        }
        out.collect(key, sum);
      }
    };
    MapReduce<Integer, String, String, Integer, Integer> mr = new MapReduceBuilder<Integer, String, String, Integer, Integer>()
            .map(new Mapper<Integer, String, String, Integer>() {
              @Override
              public void map(Integer key, String value, Collector<String, Integer> out) {
                out.collect(value, 1);
              }
            })
            .reduce(r)
            .combine(r)
            .build();

    Iterable<Pair<String, Integer>> out = mr.run(words);
    for (Pair<String, Integer> pair : out) {
      assertEquals(ref.count(pair.getKey()), pair.getValue().intValue());
    }
  }
}
