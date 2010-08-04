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

import com.tdunning.plume.types.*;

/**
 * A plume provides the runtime support for writing data-parallel programs.  Each Plume implementation
 * defines a mode of execution.  For instance, the local.eager.LocalPlume provides instant execution
 * without execution planning or any parallel implementation.
 */
public abstract class Plume {
  // general collection operations
  public abstract PCollection<String> readTextFile(String name) throws IOException;
  public abstract PCollection<String> readResourceFile(String name) throws IOException;
  public abstract <T> PCollection<T> readAvroFile(String name, PType<T> type);
  public abstract <T> PCollection<T> fromJava(Iterable<T> source);
  public abstract <T> PCollection<T> flatten(PCollection<T>... args);

  public static StringType strings() { return new StringType(); }
  public static IntegerType integers() { return new IntegerType(); }
  public static LongType longs() { return new LongType(); }
  public static FloatType floats() { return new FloatType(); }
  public static DoubleType doubles() { return new DoubleType(); }
  public static BytesType bytes() { return new BytesType(); }
  public static BooleanType booleans() { return new BooleanType(); }

  public static <K, V> PTableType<K, V> tableOf(PType<K> keyType, PType<V> valueType) {
    return new PTableType<K, V>(keyType, valueType);
  }

  public static <V> PCollectionType<V> collectionOf(PType<V> elementType) {
    return new PCollectionType<V>(elementType);
  }

  public static <T> RecordType<T> recordsOf(Class<T> recordClass) {
    return new RecordType<T>(recordClass);
  }

}
