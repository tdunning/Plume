package com.tdunning.plume.local.lazy;

import static com.tdunning.plume.Plume.collectionOf;
import static com.tdunning.plume.Plume.strings;
import static com.tdunning.plume.Plume.tableOf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Resources;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.MapRedBypassTest.MapRedBypassWorkflow;

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
        }}, tableOf(strings(), strings())).groupByKey()
        // just bring back the line (key) - doesn't do any calculation
        .map(new DoFn() {
        public void process(Object v, EmitFn emitter) {
          Pair p = (Pair)v;
          emitter.emit(p.getKey());
        }
      }, collectionOf(strings())).map(new DoFn() { // note that the optimizer should join these two sequential map() together
        public void process(Object v, EmitFn emitter) {
          Text t = (Text)v;
          // do some more foo processing         
          emitter.emit(Pair.create(t, new Text("bar")));
        }
      }, tableOf(strings(), strings()))
        // second group by key
        .groupByKey();
      
      addOutput(output);
    }
  }

  @Test
 
  @Ignore // This is work-in-progress (right now doesn't work)
  
  public void test() throws Exception {
    String outputPath = "/tmp/output-twosequentialgbktest";
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
  }
}
