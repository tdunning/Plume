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

import java.io.IOException;

/**
 * A plume provides the runtime support for writing data-parallel programs.  Each Plume implementation
 * defines a mode of execution.  For instance, the local.eager.LocalPlume provides instant execution
 * without execution planning or any parallel implementation.
 */
public abstract class Plume {
  // general collection operations
  public abstract PCollection<String> readTextFile(String name) throws IOException;
  public abstract PCollection<String> readResourceFile(String name) throws IOException;
  public abstract <T> PCollection<T> readAvroFile(String name, Class<T> targetClass);
  public abstract <T> PCollection<T> fromJava(Iterable<T> source);
  public abstract <T> PCollection<T> flatten(PCollection<T>... args);

  // conversions that signal what kind of object we want
  public <K, V> TableConversion<K, V> tableOf(Class<K> keyClass, Class<V> valueClass) {
    return null;
  }

  public <V> CollectionConversion<V> collectionOf(Class<V> valueClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public <V> CollectionConversion<V> sequenceOf(Class<V> valueClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}