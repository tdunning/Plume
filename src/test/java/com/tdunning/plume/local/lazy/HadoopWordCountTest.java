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

import static com.tdunning.plume.Plume.*;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.types.IntegerType;
import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.PTableType;
import com.tdunning.plume.types.StringType;

/**
 * Test the conversion of MSCR to MapRed by running a simple MSCR in local hadoop
 */
public class HadoopWordCountTest {

  /**
   * The WordCount Workflow
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static class WordCountWorkflow extends PlumeWorkflow {
    
    public WordCountWorkflow() {
    }
    
    @Override
    public void build() {
      init();
      
      LazyPlume plume = new LazyPlume();
      PCollection input;
      try {
        input = plume.readFile("/tmp/input-wordcount.txt", new PCollectionType(new StringType()));
        addInput(input);
      } catch (IOException e) {
        throw new RuntimeException();
      }
      
      DoFn wordCountMap = new DoFn<Text, Pair<Text, IntWritable>>() {
        @Override
        public void process(Text v,
            EmitFn<Pair<Text, IntWritable>> emitter) {
          StringTokenizer itr = new StringTokenizer(v.toString());
          while (itr.hasMoreTokens()) {
            emitter.emit(Pair.create(new Text(itr.nextToken()), new IntWritable(1)));
          }
        }
      };
      final CombinerFn wordCountCombiner = new CombinerFn<IntWritable>() {
        @Override
        public IntWritable combine(Iterable<IntWritable> stuff) {
          int c = 0;
          for(IntWritable i : stuff) {
            c += i.get();
          }
          return new IntWritable(c);
        }
      };
      DoFn wordCountReduce = new DoFn<Pair<Text, Iterable<IntWritable>>, Pair<Text, Text>>() {
        @Override
        public void process(Pair<Text, Iterable<IntWritable>> v,
            EmitFn<Pair<Text, Text>> emitter) {
          emitter.emit(Pair.create(v.getKey(), new Text(""+wordCountCombiner.combine(v.getValue()))));
        }
      };

      PCollection output = input.map(wordCountMap, tableOf(strings(), integers()))
        .groupByKey()
        .combine(wordCountCombiner)
        .map(wordCountReduce, tableOf(strings(), strings()));
      
      addOutput(output);
    }
  }
  
  /**
   * The wordcount example to test with local hadoop
   * 
   * @throws IOException 
   */
  @SuppressWarnings({ "unchecked", "deprecation" })
  @Test
  public void testWordCount() throws IOException {
    String inputPath = "/tmp/input-wordcount.txt";
    String outputPath = "/tmp/output-mscrtomapred-wordcount";
    
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    system.copyFromLocalFile(new Path(Resources.getResource("simple-text.txt").getPath()), new Path(inputPath));
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    
    // Prepare workflow
    WordCountWorkflow workFlow = new WordCountWorkflow();
    
    // Get MSCR to convert to MapRed
    Optimizer optimizer = new Optimizer();
    ExecutionStep step = optimizer.optimize(workFlow);
    MSCR mscr = step.getMscrSteps().iterator().next();

    // Run Job
    JobConf job = MSCRToMapRed.getMapRed(mscr, workFlow, "WordCount", outputPath);
    JobClient.runJob(job);
    
    List<String> str = Files.readLines(new File(outputPath+"/1-r-00000"), Charsets.UTF_8);
    
    Map<String, String> m = Maps.newHashMap();
    for (String line: str) {
      m.put(line.split("\t")[0], line.split("\t")[1]); // not super-optimal, but less code
    }
    assertEquals(3+"", m.get("is"));
    assertEquals(3+"", m.get("some"));
    assertEquals(3+"", m.get("simple"));
    assertEquals(1+"", m.get("examples"));
    assertEquals(2+"", m.get("text"));
  }
}
