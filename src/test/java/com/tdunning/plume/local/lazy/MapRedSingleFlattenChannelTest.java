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
import com.tdunning.plume.local.lazy.MapRedBypassTest.MapRedBypassWorkflow;

public class MapRedSingleFlattenChannelTest {

  /**
   * In this example we open a file and apply two functions to it. One of them performs a group by key and the other one
   *  just adds as output the result of the second function (bypass channel).
   */
  public static class MapRedSingleFlattenChannelTestWorkflow extends PlumeWorkflow {

    public MapRedSingleFlattenChannelTestWorkflow() {
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void build() {
      init();
      
      LazyPlume plume = new LazyPlume();
      PCollection input;
      PCollection input2;
      try {
        // Read input
        input = plume.readFile("/tmp/input-wordcount.txt", collectionOf(strings()));
        input2 = plume.readFile("/tmp/input-moretext.txt", collectionOf(strings()));
        // Add it as workflow's input
        addInput(input);
      } catch (IOException e) {
        throw new RuntimeException();
      }
      
      PCollection transform = input.map(new DoFn() {
        @Override
        public void process(Object v, EmitFn emitter) {
          Text t = (Text)v;
          emitter.emit(new Text(t.toString()+"-bar"));
        }}, collectionOf(strings()));
      
      addOutput(plume.flatten(input2, transform)); // flatten with another file
      
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
    String outputPath = "/tmp/output-plume-singleflattenchanneltest";
    String inputPath = "/tmp/input-wordcount.txt";
    String inputPath2 = "/tmp/input-moretext.txt";
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    system.copyFromLocalFile(new Path(Resources.getResource("simple-text.txt").getPath()), new Path(inputPath));
    system.copyFromLocalFile(new Path(Resources.getResource("simple-text.txt").getPath()), new Path(inputPath2));
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    // Prepare workflow
    MapRedSingleFlattenChannelTestWorkflow workFlow = new MapRedSingleFlattenChannelTestWorkflow();
    // Execute it
    MapRedExecutor executor = new MapRedExecutor();
    executor.execute(workFlow, outputPath);
    /**
     * TODO add test validation
     */
  }
}
