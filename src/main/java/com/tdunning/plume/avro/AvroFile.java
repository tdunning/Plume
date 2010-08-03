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

package com.tdunning.plume.avro;

import java.io.*;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.file.DataFileStream;

import com.google.common.collect.Maps;

import com.tdunning.plume.*;
import com.tdunning.plume.types.PType;
import com.tdunning.plume.local.eager.*;

/** A PCollection for an Avro file. */
public class AvroFile<T> extends PCollection<T> {
  private File file;
  private Schema schema;

  public AvroFile(String name, PType type) {
    this.file = new File(name);
    this.schema = AvroTypes.getSchema(type);
  }

  @Override
  public <R> PCollection<R> map(DoFn<T, R> fn, PType type) {
    final LocalCollection<R> r = new LocalCollection<R>();
    
    for (T t : this) {
      fn.process(t, new EmitFn<R>() {
          @Override
            public void emit(R y) {
            r.getData().add(y);
          }
        });
    }
    return r;
  }

  @Override
  public <K, V> PTable<K,V> map(DoFn<T, Pair<K,V>> fn, PType type) {
    final LocalTable<K, V> r = new LocalTable<K, V>();
    for (final T t : this) {
      fn.process(t, new EmitFn<Pair<K, V>>() {
        @Override
        public void emit(Pair<K, V> value) {
          r.getData().add(value);
        }
      });
    }
    return r;
  }

  @Override
  public PTable<T, Integer> count() {
    Map<T, Integer> x = Maps.newHashMap();
    for (T t : this) {
      Integer v = x.get(t);
      if (v == null) {
        x.put(t, 1);
      } else {
        x.put(t, v + 1);
      }
    }
    LocalTable<T, Integer> r = new LocalTable<T, Integer>();
    for (T t : x.keySet()) {
      r.getData().add(new Pair<T, Integer>(t, x.get(t)));
    }
    return r;
  }

  @Override
  public Iterator<T> iterator() {
    try {
      final DataFileStream<T> data =
        new DataFileStream<T>
        (new BufferedInputStream(new FileInputStream(file)),
         new SpecificDatumReader<T>(schema));
      // wrapper that closes the file when iteration is complete
      return new Iterator<T>() {
        public boolean hasNext() {
          boolean value = data.hasNext();
          if (!value) close();
          return value;
        }
        public T next() { return data.next(); }
        public void remove() { throw new UnsupportedOperationException(); }
        protected void finalize() { close(); }
        private void close() {
          try {
            data.close();
          } catch (IOException e){
            throw new RuntimeException(e);
          }
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
