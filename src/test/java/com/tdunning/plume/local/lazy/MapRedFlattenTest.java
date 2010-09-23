package com.tdunning.plume.local.lazy;

import static com.tdunning.plume.Plume.collectionOf;
import static com.tdunning.plume.Plume.tableOf;
import static com.tdunning.plume.Plume.strings;
import static com.tdunning.plume.Plume.integers;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Resources;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;

public class MapRedFlattenTest extends BaseTestClass {

  static String inputPathEvent2 = "/tmp/input-event2.txt";
  static String inputPathLogFile = "/tmp/input-logfile.txt";

  public static class MapRedFlattenTestWorkflow extends PlumeWorkflow {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void build() {
      init();

      LazyPlume plume = new LazyPlume();
      // Get input files
      PCollection inputEvent2, inputLogFile;
      try {
        inputEvent2  = plume.readFile(inputPathEvent2, collectionOf(strings()));
        inputLogFile = plume.readFile(inputPathLogFile, collectionOf(strings()));
        // Add as inputs
        addInput(inputEvent2);
        addInput(inputLogFile);
      } catch (IOException e) {
        throw new RuntimeException();
      }      
        
      /**
       * We use flatten to aggregate one log file we have with a list of users that used one new event.
       * The list of users is converted to the log format before flattening by adding a date and a event name.
       */
      PCollection aggregateLog = plume.flatten(
        inputLogFile,
        inputEvent2.map(new DoFn<Text, Text>() {
          @Override
          public void process(Text v, EmitFn emitter) {
            emitter.emit(new Text(new SimpleDateFormat("yyyy/MM/dd").format(new Date())+"\t"+"new_event"+"\t"+v.toString()));
          }
        }, collectionOf(strings())));
        
      /**
       * We use the aggregate log to calculate a map of [date, user] -> #clicks
       */
      PCollection dateUserClicks = aggregateLog.map(new DoFn<Text, Pair>() {
          @Override
          public void process(Text v, EmitFn<Pair> emitter) {
            String[] splittedLine = v.toString().split("\t");
            Text dateUser = new Text(splittedLine[0]+"\t"+splittedLine[2]);
            emitter.emit(Pair.create(dateUser, new IntWritable(1)));
          }
        }, tableOf(strings(), integers()))
        .groupByKey()
        .combine(countCombiner)
        .map(countReduceToText, tableOf(strings(), strings()));

      /**
       * We use the aggregate log to calculate a map of [date] -> #clicks
       */
      PCollection dateClicks = aggregateLog.map(new DoFn<Text, Pair>() {
          @Override
          public void process(Text v, EmitFn<Pair> emitter) {
            String[] splittedLine = v.toString().split("\t");
            emitter.emit(Pair.create(new Text(splittedLine[0]), new IntWritable(1)));
          }
        }, tableOf(strings(), integers()))
        .groupByKey()
        .combine(countCombiner)
        .map(countReduceToText, tableOf(strings(), strings()));
  
      /**
       * We use the aggregate log to calculate a list of uniq users
       */
      PCollection uniqUsers = aggregateLog.map(new DoFn<Text, Pair>() {
        @Override
        public void process(Text v, EmitFn<Pair> emitter) {
          String[] splittedLine = v.toString().split("\t");
          emitter.emit(Pair.create(new Text(splittedLine[2]), new Text("")));
        }
      }, tableOf(strings(), strings()))
      .groupByKey()
      .map(new DoFn<Pair, Text>() { // Reduce - just emit the key
        @Override
        public void process(Pair v, EmitFn<Text> emitter) {
          emitter.emit((Text)v.getKey());
        }
      }, collectionOf(strings()));
      
      addOutput(dateUserClicks);
      addOutput(dateClicks);
      addOutput(uniqUsers);
    }
  }
  
  @Test
  public void test() throws Exception {
    String outputPath = "/tmp/output-plume-flattentest";
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    system.copyFromLocalFile(new Path(Resources.getResource("event2users.txt").getPath()), new Path(inputPathEvent2));
    system.copyFromLocalFile(new Path(Resources.getResource("eventslog.txt").getPath()), new Path(inputPathLogFile));
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    // Prepare workflow
    MapRedFlattenTestWorkflow workFlow = new MapRedFlattenTestWorkflow();
    // Execute it
    MapRedExecutor executor = new MapRedExecutor();
    executor.execute(workFlow, outputPath);
  }
}
