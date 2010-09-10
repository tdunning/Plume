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
 * This test asserts that bypass channels as described in FlumeJava paper work for Plume MSCRs.
 */
public class MapRedBypassTest {

  /**
   * In this example we open a file and apply two functions to it. One of them performs a group by key and the other one
   *  just adds as output the result of the second function (bypass channel).
   */
  public static class MapRedBypassWorkflow extends PlumeWorkflow {

    public MapRedBypassWorkflow() {
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
      
      PCollection bypassTransform = input.map(new DoFn() {
        @Override
        public void process(Object v, EmitFn emitter) {
          Text t = (Text)v;
          emitter.emit(Pair.create(new Text(t + "-blah"), new Text(t + "-bloh")));
        }}, tableOf(strings(), strings()));
      
      addOutput(bypassTransform);
      
      PCollection groupedTransform = input.map(new DoFn() {
        @Override
        public void process(Object v, EmitFn emitter) {
          Text t = (Text)v;
          emitter.emit(Pair.create(t, new Text("foo")));
        }}, tableOf(strings(), strings())).groupByKey();
      
      addOutput(groupedTransform);
    }
  }

  @Test
  public void test() throws Exception {
    String outputPath = "/tmp/output-bypasstest";
    String inputPath = "/tmp/input-wordcount.txt";
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    system.copyFromLocalFile(new Path(Resources.getResource("simple-text.txt").getPath()), new Path(inputPath));
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    // Prepare workflow
    MapRedBypassWorkflow workFlow = new MapRedBypassWorkflow();
    // Execute it
    MapRedExecutor executor = new MapRedExecutor();
    executor.execute(workFlow, outputPath);
    
    List<String> str = Files.readLines(new File(outputPath+"/1/1-r-00000"), Charsets.UTF_8);
    Map<String, String> m = Maps.newHashMap();
    for (String line: str) {
      m.put(line.split("\t")[0], line.split("\t")[1]); // not super-optimal, but less code
    }
    assertEquals(m.get("To test text processing with some simple-blah"), "To test text processing with some simple-bloh");
    assertEquals(m.get("some simple text-blah"), "some simple text-bloh");
    assertEquals(m.get("is is-blah"), "is is-bloh");
    
    str = Files.readLines(new File(outputPath+"/1/2-r-00000"), Charsets.UTF_8);
    m = Maps.newHashMap();
    for (String line: str) {
      m.put(line.split("\t")[0], line.split("\t")[1]); // not super-optimal, but less code
    }
    assertEquals(m.get("To test text processing with some simple"), "foo");
    assertEquals(m.get("some simple text"), "foo");
    assertEquals(m.get("is is"), "foo");
  }
}
