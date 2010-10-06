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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.io.Resources;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;

/**
 * This test checks that an MSCR is created out of a workplan that consists only of Flatten operations. There is more than one
 * {@link Flatten} and these have co-related inputs, so they have to be merged into a single MSCR.
 */
public class MapRedOnlyFlattensTest {
  
  static String inputPathEvent2 = "/tmp/input-event2.txt";
  static String inputPathLogFile2 = "/tmp/input-logfile2.txt";
  static String inputPathLogFile = "/tmp/input-logfile.txt";
  
  public static class MapRedOnlyFlattensTestWorkflow extends PlumeWorkflow {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void build() {
      init();

      LazyPlume plume = new LazyPlume();
      // Get input files
      PCollection inputEvent2, inputLogFile, inputLogFile2;
      try {
        inputEvent2   = plume.readFile(inputPathEvent2, collectionOf(strings()));
        inputLogFile2 = plume.readFile(inputPathLogFile2, collectionOf(strings()));
        inputLogFile  = plume.readFile(inputPathLogFile, collectionOf(strings()));
        // Add as inputs
        addInput(inputEvent2);
        addInput(inputLogFile);
      } catch (IOException e) {
        throw new RuntimeException();
      }      
      
      /**
       * Emit the user of the log file - flatten it with users file
       */
      PCollection output = plume.flatten(collectionOf(strings()),
        inputEvent2,
        inputLogFile.map(new DoFn<Text, Text>() {
          @Override
          public void process(Text v, EmitFn<Text> emitter) {
            String[] splittedLine = v.toString().split("\t");
            emitter.emit(new Text(splittedLine[2]));
          }
        }, collectionOf(strings())));
      
      /**
       * Flatten two log files
       */
      PCollection output2 = plume.flatten(collectionOf(strings()),
        inputLogFile2,
        inputLogFile);
      
      addOutput(output);
      addOutput(output2);
    }
  }
  
  @Test
  public void test() throws Exception {
    String outputPath = "/tmp/output-plume-onlyflattentest";
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    system.copyFromLocalFile(new Path(Resources.getResource("event2users.txt").getPath()), new Path(inputPathEvent2));
    system.copyFromLocalFile(new Path(Resources.getResource("eventslog.txt").getPath()), new Path(inputPathLogFile));
    system.copyFromLocalFile(new Path(Resources.getResource("eventslog.txt").getPath()), new Path(inputPathLogFile2));
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    // Prepare workflow
    MapRedOnlyFlattensTestWorkflow workFlow = new MapRedOnlyFlattensTestWorkflow();
    // Execute it
    MapRedExecutor executor = new MapRedExecutor();
    executor.execute(workFlow, outputPath);
  }
}
