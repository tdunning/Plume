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

package com.tdunning.plume.local.lazy;

import static com.tdunning.plume.Plume.collectionOf;
import static com.tdunning.plume.Plume.strings;
import static com.tdunning.plume.Plume.tableOf;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;

/**
 * This test asserts that Plume creates two MSCR and therefore two MapReduce jobs when one Group By Key follows another
 */
public class MapRedTwoSequentialGBKTest {

  public static class TwoSequentialGBKWorkflow extends PlumeWorkflow {

    public TwoSequentialGBKWorkflow() {
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void build() {
      init();
      
      LazyPlume plume = new LazyPlume();
      PCollection input;
      try {
        // Read input
        input = plume.readFile("/tmp/input-wordcount.txt", collectionOf(strings()));
        // Add it as workflow's input
        addInput(input);
      } catch (IOException e) {
        throw new RuntimeException();
      }
      
      PCollection output = input.map(new DoFn() {
        @Override
        public void process(Object v, EmitFn emitter) {
          Text t = (Text)v;
          // do some foo processing
          emitter.emit(Pair.create(t, new Text("foo")));
        }}, tableOf(strings(), strings()))
         .groupByKey()
         .map(new DoFn() { 
        public void process(Object v, EmitFn emitter) {
          Pair p = (Pair)v;
          // do some more foo processing         
          emitter.emit(Pair.create(p.getKey(), new Text("bar")));
        }
      }, tableOf(strings(), strings()))
        // second group by key
         .groupByKey()
         .map(new DoFn() { 
        public void process(Object v, EmitFn emitter) {
          Pair p = (Pair)v;
          // do some more foo processing         
          emitter.emit(Pair.create(p.getKey(), new Text("bar 2")));
        }
      }, tableOf(strings(), strings()));
      
      addOutput(output);
    }
  }

  @Test
  public void test() throws Exception {
    String outputPath = "/tmp/output-plume-twosequentialgbktest";
    String inputPath = "/tmp/input-wordcount.txt";
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    system.copyFromLocalFile(new Path(Resources.getResource("simple-text.txt").getPath()), new Path(inputPath));
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    // Prepare workflow
    TwoSequentialGBKWorkflow workFlow = new TwoSequentialGBKWorkflow();

    // Execute it
    MapRedExecutor executor = new MapRedExecutor();
    executor.execute(workFlow, outputPath);
    
    String outputId = ((LazyCollection<?>)workFlow.getOutputs().get(0)).getPlumeId();
    List<String> str = Files.readLines(new File(outputPath+"/"+outputId+"/1-r-00000"), Charsets.UTF_8);
    
    Map<String, String> m = Maps.newHashMap();
    for (String line: str) {
      m.put(line.split("\t")[0], line.split("\t")[1]); // not super-optimal, but less code
    }
    assertEquals("bar 2", m.get("To test text processing with some simple"));
    assertEquals("bar 2", m.get("examples is some simple"));
    assertEquals("bar 2", m.get("is is"));
    assertEquals("bar 2", m.get("some simple text"));
  }
}
