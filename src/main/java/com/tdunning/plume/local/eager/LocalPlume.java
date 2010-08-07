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

package com.tdunning.plume.local.eager;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.tdunning.plume.*;
import com.tdunning.plume.types.*;
import com.tdunning.plume.avro.*;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Local plume runtime.  All files are local, all tasks are run in threads.  Currently threads == thread
 */
public class LocalPlume extends Plume {
  @Override
  public LocalCollection<String> readTextFile(String name) throws IOException {
    return LocalCollection.wrap(Files.readLines(new File(name), Charsets.UTF_8));
  }

  @Override
  public PCollection<String> readResourceFile(String name) throws IOException {
    return LocalCollection.wrap(Resources.readLines(Resources.getResource(name), Charsets.UTF_8));
  }

  @Override
  public <T> PCollection<T> readAvroFile(String name, PType<T> type) {
    return new AvroFile<T>(name, type);
  }

  @Override
  public <X> LocalCollection<X> fromJava(Iterable<X> data) {
    return new LocalCollection<X>().addAll(data);
  }

  @Override
  public <T> PCollection<T> flatten(PCollection<T>... args) {
    LocalCollection<T> r = new LocalCollection<T>();
    for (PCollection<T> arg : args) {
      if (arg instanceof LocalCollection) {
        r.addAll(((LocalCollection<T>) arg).getData());
      } else {
        throw new UnsupportedOperationException("Can't flatten a " + arg.getClass() + " onto local collections");
      }
    }
    return r;
  }

  @Override
  public <T> void writeAvroFile(String name, PCollection<T> data, PType<T> type) throws IOException {
    Schema schema = AvroTypes.getSchema(type);
    DataFileWriter<T> factory = new DataFileWriter<T>(new SpecificDatumWriter<T>(schema));
    DataFileWriter<T> out = factory.create(schema, new File(name));
    for (T t : data) {
      out.append(t);
    }
    out.close();
  }
}
