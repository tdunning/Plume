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
 * Created by IntelliJ IDEA. User: tdunning Date: Aug 2, 2010 Time: 6:04:58 PM To change this
 * template use File | Settings | File Templates.
 */
public interface Reducer<K, V, V1> {
  public void reduce(K key, Iterable<V> values, Collector<K, V1> collector);
}
