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

import static com.tdunning.plume.Plume.integers;
import static com.tdunning.plume.Plume.tableOf;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Files;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;

/**
 * The purpose of this test is to assert that {@link MapRedExecutor} behaves correctly with SequenceFile as input/output
 */
public class MapRedSequenceFileTest {

  final static String inputPath = "/tmp/input-simpletest/file";
  
  /**
   * This workflow will only load a <int,int> table and add one to each number
   */
  public static class OtherWorkflow extends PlumeWorkflow {

    public OtherWorkflow() {
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void build() {
      init();
      LazyPlume plume = new LazyPlume();
      PCollection input;
      try {
        // Get input file
        input = plume.readFile(inputPath, tableOf(integers(), integers()));
        // Add as input for this workflow
        addInput(input);
      } catch (IOException e) {
       throw new RuntimeException(e);
      }
      // Define its output
      PCollection output = input.map(new DoFn<Pair<IntWritable, IntWritable>, Pair<IntWritable, IntWritable>>() {
        @Override
        public void process(Pair<IntWritable, IntWritable> v,
            EmitFn<Pair<IntWritable, IntWritable>> emitter) {
          emitter.emit(Pair.create(new IntWritable(v.getKey().get() + 1), new IntWritable(v.getValue().get() + 1)));
        }
      }, tableOf(integers(), integers())).groupByKey();
      
      // Add it as workflow's output
      addOutput(output);
    }
  }

  @Test
  public void test() throws Exception {
    /*
     * Create input which is SequenceFile<int,int> with data 1,2\n3,4
     */
    Configuration conf = new Configuration();
    Path p = new Path(inputPath);
    FileSystem localFS = FileSystem.getLocal(conf);
    if(localFS.exists(p)) { 
      localFS.delete(p, true); // wipe it if needed
    }
    SequenceFile.Writer writer = SequenceFile.createWriter(localFS, conf, p, IntWritable.class, IntWritable.class);
    writer.append(new IntWritable(1), new IntWritable(2));
    writer.append(new IntWritable(3), new IntWritable(4));
    writer.close();
    String outputPath = "/tmp/output-plume-simpletest";
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    // Prepare workflow
    OtherWorkflow workFlow = new OtherWorkflow();
    // Execute it
    MapRedExecutor executor = new MapRedExecutor();
    executor.execute(workFlow, outputPath);
    /*
     * Read output which is SequenceFile<int,int> and assert that it has data 2,3\n4,5
     */
    p = new Path(outputPath + "/1_1/1-r-00000");
    SequenceFile.Reader reader = new SequenceFile.Reader(localFS, p, conf);
    IntWritable key = new IntWritable(1);
    IntWritable value = new IntWritable(1);
    reader.next(key, value);
    assertEquals(key.get(), 2);
    assertEquals(value.get(), 3);
    reader.next(key, value);
    assertEquals(key.get(), 4);
    assertEquals(value.get(), 5);
    reader.close();
  }
}
