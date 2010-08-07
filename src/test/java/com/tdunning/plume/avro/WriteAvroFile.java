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

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Aug 4, 2010 Time: 6:32:18 PM To change this
 * template use File | Settings | File Templates.
 */
public class WriteAvroFile {
  @Test
  public void writeSomething() throws IOException {
    Schema s = Schema.create(Schema.Type.DOUBLE);
    DataFileWriter<Double> x = new DataFileWriter<Double>(new SpecificDatumWriter<Double>(s));
    File f = new File("x");
    DataFileWriter<Double> z = x.create(s, f);
    f.deleteOnExit();
    for (int i = 0; i < 10; i++) {
      z.append(3.0*i);
    }
    z.close();

    DataFileReader<Double> in = new DataFileReader<Double>(new File("x"),
            new SpecificDatumReader<Double>(s));
    int k = 0;
    while (in.hasNext()) {
      assertEquals(3.0 * k++, in.next(), 0);
    }
    in.close();

    final DataFileStream<Double> data =
      new DataFileStream<Double>
      (new BufferedInputStream(new FileInputStream("x")),
       new SpecificDatumReader<Double>(s));
    k = 0;
    while (data.hasNext()) {
      assertEquals(3.0 * k++, data.next(), 0);
    }
    data.close();
  }
}
