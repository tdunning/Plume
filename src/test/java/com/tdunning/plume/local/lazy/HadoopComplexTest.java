package com.tdunning.plume.local.lazy;

import static com.tdunning.plume.Plume.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.google.common.io.Resources;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.StringType;

public class HadoopComplexTest {

  public static class ComplexWorkflow extends PlumeWorkflow {

    @SuppressWarnings("unchecked")
    @Override
    public void build() {
      init();
      
      LazyPlume plume = new LazyPlume();
      PCollection input;
      try {
        input = plume.readFile("/tmp/input-wordcount.txt", collectionOf(strings()));
        addInput(input);
      } catch (IOException e) {
        throw new RuntimeException();
      }
      
      final IntWritable one = new IntWritable(1);
      
      DoFn<Pair<WritableComparable, Iterable<IntWritable>>, Pair<WritableComparable, IntWritable>> countReduce = 
        new DoFn<Pair<WritableComparable, Iterable<IntWritable>>, Pair<WritableComparable, IntWritable>>() {
        @Override
        public void process(Pair<WritableComparable, Iterable<IntWritable>> v,
            EmitFn<Pair<WritableComparable, IntWritable>> emitter) {
          Iterator<IntWritable> it = v.getValue().iterator();
          int count = 0;
          while(it.hasNext()) {
            count += it.next().get();
          }
          emitter.emit(Pair.create(v.getKey(), new IntWritable(count)));
        }
      };
      
      // Count and group by #chars of line
      PCollection po1 = input.map(new DoFn<Text, Pair<IntWritable, IntWritable>>() {
        @Override
        public void process(Text v, EmitFn<Pair<IntWritable, IntWritable>> emitter) {
          StringTokenizer itr = new StringTokenizer(v.toString());
          int length = 0;
          while (itr.hasMoreTokens()) {
            length += itr.nextToken().length();
          }
          emitter.emit( Pair.create(new IntWritable(length), one) );
        }
      }, tableOf(integers(), integers()))
       .groupByKey()
       .map(countReduce, tableOf(integers(), integers()));
      
      // Count and group by #tokens of line
      PCollection po2 = input.map(new DoFn<Text, Pair<IntWritable, IntWritable>>() {
        @Override
        public void process(Text v, EmitFn<Pair<IntWritable, IntWritable>> emitter) {
          StringTokenizer itr = new StringTokenizer(v.toString());
          int length = 0;
          while (itr.hasMoreTokens()) {
            length ++;
            itr.nextToken();
          }
          emitter.emit( Pair.create(new IntWritable(length), one) );
        }
      }, tableOf(integers(), integers()))
       .groupByKey()
       .map(countReduce, tableOf(integers(), integers()));
      
      // Count appearances of chars
      PCollection po3 = input.map(new DoFn<Text, Pair<Text, IntWritable>>() {
        @Override
        public void process(Text v, EmitFn<Pair<Text, IntWritable>> emitter) {
          StringTokenizer itr = new StringTokenizer(v.toString());
          while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            for(int i = 0; i < token.length(); i++) {
              emitter.emit( Pair.create(new Text(token.charAt(i)+""), one) );
            }
          }
        }
      }, tableOf(strings(), integers()))
       .groupByKey()
       .map(countReduce, tableOf(strings(), integers()));
      
      addOutput(po1);
      addOutput(po2);
      addOutput(po3);
    }
  }
  
  @Test
  public void test() throws IOException {
    String inputPath = "/tmp/input-wordcount.txt";
    String outputPath = "/tmp/output-mscrtomapred-complex";
    
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    system.copyFromLocalFile(new Path(Resources.getResource("simple-text.txt").getPath()), new Path(inputPath));
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    
    // Prepare workflow
    ComplexWorkflow workFlow = new ComplexWorkflow();
    
    // Get MSCR to convert to MapRed
    Optimizer optimizer = new Optimizer();
    ExecutionStep step = optimizer.optimize(workFlow);
    MSCR mscr = step.getMscrSteps().iterator().next();

    // Run Job
    JobConf job = MSCRToMapRed.getMapRed(mscr, workFlow, "Complex", outputPath);
    JobClient.runJob(job);      
  }
}
