package com.tdunning.plume.local.lazy;

import static com.tdunning.plume.Plume.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.google.common.io.Resources;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.HadoopWordCountTest.WordCountWorkflow;

public class HadoopSimpleTest {

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
        input = plume.readFile("/tmp/output-mscrtomapred-complex/2-r-00000", tableOf(integers(), integers()));
        addInput(input);
      } catch (IOException e) {
       throw new RuntimeException(e);
      }
      PCollection output = input.map(new DoFn<Pair<IntWritable, IntWritable>, Pair<IntWritable, IntWritable>>() {
        @Override
        public void process(Pair<IntWritable, IntWritable> v,
            EmitFn<Pair<IntWritable, IntWritable>> emitter) {
          emitter.emit(Pair.create(new IntWritable(v.getKey().get() + 1), new IntWritable(v.getValue().get() + 1)));
        }
      }, tableOf(integers(), integers())).groupByKey();
      
      addOutput(output);
    }
  }
  
  @Test
  public void test() throws IOException {
    String outputPath = "/tmp/output-simpletest";
    // Prepare input for test
    FileSystem system = FileSystem.getLocal(new Configuration());
    // Prepare output for test
    system.delete(new Path(outputPath), true);
    // Prepare workflow
    OtherWorkflow workFlow = new OtherWorkflow();
    // Get MSCR to convert to MapRed
    Optimizer optimizer = new Optimizer();
    ExecutionStep step = optimizer.optimize(workFlow);
    MSCR mscr = step.getMscrSteps().iterator().next();
    // Run Job
    JobConf job = MSCRToMapRed.getMapRed(mscr, workFlow, "Simple", outputPath);
    JobClient.runJob(job);
  }
}
