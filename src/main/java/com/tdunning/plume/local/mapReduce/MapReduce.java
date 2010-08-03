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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tdunning.plume.Pair;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Aug 2, 2010 Time: 6:13:40 PM To change this
 * template use File | Settings | File Templates.
 */
public class MapReduce<MK, MV, RK, RV, OV> {
  private Mapper<MK, MV, RK, RV> mapper;
  private Reducer<RK, RV, OV> reducer;
  private Reducer<RK, RV, RV> combiner;

  public MapReduce(Mapper<MK, MV, RK, RV> mapper, Reducer<RK, RV, OV> reducer, Reducer<RK, RV, RV> combiner) {
    this.mapper = mapper;
    this.reducer = reducer;
    this.combiner = combiner;
  }
  
  public Iterable<Pair<RK, OV>> run(Iterable<Pair<MK, MV>> input) {
    final Map<RK, List<RV>> shuffle = Maps.newHashMap();
    for (Pair<MK, MV> pair : input) {
      mapper.map(pair.getKey(), pair.getValue(), new Collector<RK, RV>() {
        @Override
        public void collect(RK key, RV value) {
          List<RV> tmp = shuffle.get(key);
          if (tmp == null) {
            tmp = Lists.newArrayList();
            shuffle.put(key, tmp);
          }
          tmp.add(value);
          if (combiner != null && tmp.size() > 10) {
            final List<RV> internalBuffer = tmp;
            combiner.reduce(key, tmp, new Collector<RK, RV>() {
              @Override
              public void collect(RK key, RV value) {
                internalBuffer.clear();
                internalBuffer.add(value);
              }
            });
          }
        }
      });
    }

    final List<Pair<RK, OV>> output = Lists.newArrayList();
    for (RK key : shuffle.keySet()) {
      if (reducer == null) {
        for (RV x : shuffle.get(key)) {
          output.add(Pair.create(key, (OV) x));
        }
      } else {
        reducer.reduce(key, shuffle.get(key), new Collector<RK, OV>() {
          @Override
          public void collect(RK key, OV value) {
            output.add(Pair.create(key, value));
          }
        });
      }
    }
    return output;
  }
}
