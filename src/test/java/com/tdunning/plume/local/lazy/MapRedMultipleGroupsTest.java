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
import static com.tdunning.plume.Plume.integers;
import static com.tdunning.plume.Plume.strings;
import static com.tdunning.plume.Plume.tableOf;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import com.google.common.io.Resources;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;

/**
 * This test shows how three different group by's can be performed using Plume API which output three different
 * things and end up being executed in a single Map/Red job.
 */
public class MapRedMultipleGroupsTest extends BaseTestClass {

  /**
   * The PlumeWorkflow class is extendend so that it can be instantiated via reflection at hadoop mappers/reducers
   */
  public static class MultipleGroupsWorkflow extends PlumeWorkflow {

    @SuppressWarnings("unchecked")
    @Override
    public void build() {
      init();
      
      // Get one input file
      LazyPlume plume = new LazyPlume();
      PCollection input;
      try {
        input = plume.readFile("/tmp/input-wordcount.txt", collectionOf(strings()));
        // Add as input
        addInput(input);
      } catch (IOException e) {
        throw new RuntimeException();
      }
      
      final IntWritable one = new IntWritable(1);
      
      // Define a map that counts and group by #chars of line
      PCollection po1 = input.map(new DoFn() {
        @Override
        public void process(Object v, EmitFn emitter) {
          StringTokenizer itr = new StringTokenizer(v.toString());
          int length = 0;
          while (itr.hasMoreTokens()) {
            length += itr.nextToken().length();
          }
          emitter.emit(Pair.create(new IntWritable(length), one));
        }
      }, tableOf(integers(), integers()))
       .groupByKey()
       .map(countReduceToText, tableOf(integers(), strings()));
      
      // Define a map that counts and group by #tokens of line
      PCollection po2 = input.map(new DoFn() {
        @Override
        public void process(Object v, EmitFn emitter) {
          StringTokenizer itr = new StringTokenizer(v.toString());
          int length = 0;
          while (itr.hasMoreTokens()) {
            length ++;
            itr.nextToken();
          }
          emitter.emit(Pair.create(new IntWritable(length), one));
        }
      }, tableOf(integers(), integers()))
       .groupByKey()
       .map(countReduceToText, tableOf(integers(), strings()));
      
      // Define a map that counts appearances of chars
      PCollection po3 = input.map(new DoFn() {
        @Override
        public void process(Object v, EmitFn emitter) {
          StringTokenizer itr = new StringTokenizer(v.toString());
          while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            for(int i = 0; i < token.length(); i++) {
              emitter.emit(Pair.create(new Text(token.charAt(i)+""), one));
            }
          }
        }
      }, tableOf(strings(), integers()))
       .groupByKey()
       .map(countReduceToText, tableOf(strings(), strings()));
      
      // Add the output of the three group by's to this workflow's outputs
      addOutput(po1);
      addOutput(po2);
      addOutput(po3);
    }
  }
  
  @Test
  public void test() throws IOException, InterruptedException, ClassNotFoundException {
    String inputPath = "/tmp/input-wordcount.txt";
    String outputPath = "/tmp/output-plume-complex";
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    system.copyFromLocalFile(new Path(Resources.getResource("simple-text.txt").getPath()), new Path(inputPath));
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    // Prepare workflow
    MultipleGroupsWorkflow workFlow = new MultipleGroupsWorkflow();
    // Execute it
    MapRedExecutor executor = new MapRedExecutor();
    executor.execute(workFlow, outputPath);
    
    // Just assert that 3 output files were written and have content
    for(int i = 1; i <= 3; i++) {
      File f = new File(outputPath+"/1_" + i + "/" + i +"-r-00000");
      assertTrue(f.exists());
      assertTrue(f.length() > 100);
    }
  }
}
