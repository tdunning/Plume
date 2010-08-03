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

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Aug 2, 2010 Time: 6:08:32 PM To change this
 * template use File | Settings | File Templates.
 */
public class MapReduceBuilder<MK, MV, RK, RV, OV> {
  private Mapper<MK, MV, RK, RV> mapper;
  private Reducer<RK, RV, OV> reducer;
  private Reducer<RK, RV, RV> combiner;

  public MapReduceBuilder() {
  }

  public MapReduceBuilder<MK, MV, RK, RV, OV> map(Mapper<MK, MV, RK, RV> mapper) {
    this.mapper = mapper;
    return this;
  }

  public MapReduceBuilder<MK, MV, RK, RV, OV> reduce(Reducer<RK, RV, OV> reducer) {
    this.reducer = reducer;
    return this;
  }

  public MapReduceBuilder<MK, MV, RK, RV, OV> combine(Reducer<RK, RV, RV> combiner) {
    this.combiner = combiner;
    return this;
  }

  public MapReduce<MK, MV, RK, RV, OV> build() {
    return new MapReduce(mapper, reducer, combiner);
  }
}
