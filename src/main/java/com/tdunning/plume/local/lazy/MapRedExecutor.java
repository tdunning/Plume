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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.io.Files;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.types.BooleanType;
import com.tdunning.plume.types.BytesType;
import com.tdunning.plume.types.DoubleType;
import com.tdunning.plume.types.FloatType;
import com.tdunning.plume.types.IntegerType;
import com.tdunning.plume.types.LongType;
import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.PTableType;
import com.tdunning.plume.types.PType;
import com.tdunning.plume.types.StringType;

/**
 * This executor can be used to execute a {@link PlumeWorkflow} by using Hadoop Map-Reduce implementation. 
 * Right now it's only tested with local Hadoop.
 */
public class MapRedExecutor {

  final static String WORKFLOW_NAME = "plume.workflow.name"; // hadoop conf. property used to instantiate proper workflow
  final static String MSCR_ID = "plume.workflow.mscr.id"; // Identifies current MSCR being executed
  final static String TEMP_OUTPUT_PATH = "plume.tmp.path";
  
  String tmpOutputFolder;
  ExecutorService executor = Executors.newCachedThreadPool();
  
  final static Logger log = Logger.getLogger(MapRedExecutor.class);
  
  /**
   * Wrapper class for multi-type shuffling
   *
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  static class PlumeObject implements WritableComparable<PlumeObject> {

    WritableComparable obj;
    int sourceId; // to identify its output channel

    public PlumeObject() {

    }

    public PlumeObject(WritableComparable obj, int sourceId) {
      this.obj = obj;
      this.sourceId = sourceId;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
      String className = arg0.readUTF();
      try {
        obj = (WritableComparable) Class.forName(className).newInstance();
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      obj.readFields(arg0);
      sourceId = arg0.readInt();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      arg0.writeUTF(obj.getClass().getName());
      obj.write(arg0);
      arg0.writeInt(sourceId);
    }

    @Override
    public int compareTo(PlumeObject arg0) {
      if(arg0.sourceId != sourceId) {
        return -1;
      }
      if(arg0.obj.getClass().equals(obj.getClass())) {
        return obj.compareTo(arg0.obj);
      } else {
        return -1;
      }
    }
  }
    
  public MapRedExecutor() {
    this("/tmp/plume-mrexecutor");
  }

  /**
   * This constructor can be used to change temporary output folder. By default, it will use /tmp
   * 
   * @param outputFolder
   */
  public MapRedExecutor(String tmpOutputFolder) {
    this.tmpOutputFolder = tmpOutputFolder;
    File f = new File(this.tmpOutputFolder);
    if(f.exists()) {
      try {
        Files.deleteRecursively(f);
      } catch (IOException e) {
        log.error(e);
      }
    }
  }

  /**
   * This method can be called to execute a {@link PlumeWorkflow} by using Hadoop Map-Reduce implementation.
   * It will build the execution tree, optimize it and convert each MSCR step into a MapRed job. 
   * It will launch MSCR jobs in parallel when it is allowable to do so by using a ThreadPool. If one MSCR fails,
   * all the work flow is canceled. Because it stores the result in a temporary folder, it will only flush the final
   * result to the API parameter if the work flow has been executed successfully.
   * 
   * @param workFlow The {@link PlumeWorkflow} to execute 
   * @param outputTo Output folder where the result of the work flow will be stored if executed successfully
   * 
   * @throws IOException If the work flow had to be canceled
   * @throws InterruptedException 
   */
  public void execute(PlumeWorkflow workFlow, String outputTo) throws IOException, InterruptedException {
    Optimizer optimizer = new Optimizer();
    ExecutionStep step = optimizer.optimize(workFlow);
    int nStep = 0;
    final String workFlowId = workFlow.getClass().getName() + "-" + System.currentTimeMillis();
    do {
      nStep ++;
      log.info("Begin execution step " + nStep + " for workflow " + workFlow.getClass().getName());
      // Create a latch to mark the end of a concurrent step where all MSCRs can be executed in parallel
      final CountDownLatch latch = new CountDownLatch(step.mscrSteps.size());
      // Create a signal that can be flagged if one of the MSCRs fail to abort all the workFlow
      // - I have chosen an AtomicBoolean in case this flag can be re-set to false under some circumstance -
      final AtomicBoolean abort  = new AtomicBoolean(false);
      // For each MSCR that can be executed concurrently...
      for(final MSCR mscr: step.mscrSteps) {
        final String workFlowOutputPath = tmpOutputFolder + "/" + workFlowId;
        final String jobId = workFlowId + "/" + mscr.getId();
        final String jobOutputPath = tmpOutputFolder + "/" + jobId;
        log.info("Triggering execution of jobId " + jobId + ". Its output will be saved to " + jobOutputPath);
        // ... Get its MapRed Job
        final Job job = getMapRed(mscr, workFlow, workFlowOutputPath, jobOutputPath);
        final FileSystem fS = FileSystem.getLocal(job.getConfiguration());
        // ... Submit it to the ThreadPool
        executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              job.waitForCompletion(true);
              // job completed successfully - materialize outputs
              log.info("jobId " + jobId + " completed successfully, now materializing outputs.");
              for(Map.Entry<PCollection<?>, Integer> entry: mscr.getNumberedChannels().entrySet()) {
                LazyCollection<?> oCol = (LazyCollection<?>)mscr.getOutputChannels().get(entry.getKey()).output;
                // Move this output to somewhere recognizable - this executor's tmp folder + this PCollection's Plume Id
                // This way, mappers that read unmaterialized collections will know where to find intermediate states.
                FileStatus[] files = fS.listStatus(new Path(jobOutputPath));
                Path materializedPath = new Path(workFlowOutputPath + "/" + oCol.getPlumeId());
                fS.mkdirs(materializedPath);
                for(FileStatus file: files) {
                  if(file.getPath().getName().startsWith(entry.getValue()+"-r-")) {
                    FileUtil.copy(fS, file.getPath(), fS, materializedPath, true, job.getConfiguration());
                    oCol.setFile(materializedPath.toString());
                  }
                }
                log.info("Materialized plume output " + oCol.getPlumeId() + " to " + oCol.getFile());
              }
            } catch (IOException e) {
              log.warn("One Job failed: " + jobId + ", current Workflow will be aborted ", e);
              abort.set(true); // Flag the premature end of this workflow
            } catch (InterruptedException e) {
              log.warn("One Job failed: " + jobId + ", current Workflow will be aborted ", e);
              abort.set(true); // Flag the premature end of this workflow
            } catch (ClassNotFoundException e) {
              log.warn("One Job failed: " + jobId + ", current Workflow will be aborted ", e);
              abort.set(true); // Flag the premature end of this workflow
            } finally {
              latch.countDown(); // Count down under any circumstance
            }
          }
        });
        latch.await(); // wait until all MSCRs from this step are completed
        if(abort.get()) {
          throw new IOException("Current Workflow was aborted");
        }
      }
      step = step.nextStep;
    } while(step != null);    
    log.info("Workflow ended correctly.");
    // Move temporary result to where API user wants to: WARN: Local-specific implementation
    Files.move(new File(tmpOutputFolder + "/" + workFlowId ), new File(outputTo));
  }

  /**
   * This method returns a Job instance out of a {@link MSCR} entity. It puts the Class of 
   * the {@link PlumeWorkflow} argument and the MSCR id in the hadoop configuration.
   * 
   * @param mscr The MSCR to convert 
   * @param workflow The workflow whose class will be instantiated by hadoop mappers/reducers
   * @param outputPath The output path of the MapRed job
   * @return A hadoop-executable MapRed Job
   * 
   * @throws IOException
   */
  static Job getMapRed(final MSCR mscr, PlumeWorkflow workFlow, String workFlowOutputPath, String outputPath) 
    throws IOException {
    
    Configuration conf = new Configuration();
    conf.set(WORKFLOW_NAME, workFlow.getClass().getName());
    conf.setInt(MSCR_ID, mscr.getId());
    conf.set(TEMP_OUTPUT_PATH, workFlowOutputPath);

    Job job = new Job(conf, "MSCR"); // TODO deprecation

    job.setMapOutputKeyClass(PlumeObject.class);
    job.setMapOutputValueClass(PlumeObject.class);

    job.setJarByClass(MapRedExecutor.class);
    
    /**
     * Define multiple inputs
     */
    for(PCollection<?> input: mscr.getInputs()) {
      if(!(input instanceof LazyCollection)) {
        throw new IllegalArgumentException("Can't create MapRed from MSCR whose inputs are not LazyTable");
      }
      LazyCollection<Text> l = (LazyCollection<Text>)input;
      if(!(l.isMaterialized() && l.getFile() != null)) {
        // Collections have plume ID only if they are intermediate results - TODO better naming for this
        if(l.getPlumeId().length() < 1) {
          throw new IllegalArgumentException("Can't create MapRed from MSCR inputs that are not materialized to a file");
        }
      }
      PCollectionType<?> rType = l.getType();
      Class<? extends InputFormat> format =  SequenceFileInputFormat.class;
      if(rType instanceof PTableType) {
        PTableType<?, ?> tType = (PTableType<?, ?>)rType;
        if(tType.valueType() instanceof StringType && tType.keyType() instanceof StringType) {
          format = KeyValueTextInputFormat.class;
        }
        MultipleInputs.addInputPath(job, new Path(l.getFile()), format, MSCRMapper.class);        
      } else {
        if(rType.elementType() instanceof StringType) {
          format = TextInputFormat.class;
        }
        MultipleInputs.addInputPath(job, new Path(l.getFile()), format, MSCRMapper.class);
      }
    }
    /**
     * Define multiple outputs
     */
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    for(Map.Entry<PCollection<?>, Integer> entry: mscr.getNumberedChannels().entrySet()) {
      PCollectionType<?> rType = ((LazyCollection<?>)mscr.getOutputChannels().get(entry.getKey()).output).getType();
      if(rType instanceof PTableType) {
        PTableType<?, ?> tType = (PTableType<?, ?>)rType;
        Class<? extends OutputFormat> outputFormat = SequenceFileOutputFormat.class;
        if(tType.keyType() instanceof StringType && tType.valueType() instanceof StringType) {
          outputFormat = TextOutputFormat.class;
        }
        MultipleOutputs.addNamedOutput(job, entry.getValue()+"", outputFormat, 
            getHadoopType(tType.keyType()), getHadoopType(tType.valueType()));
      } else {
        Class<? extends OutputFormat> outputFormat = SequenceFileOutputFormat.class;
        if(rType.elementType() instanceof StringType) {
          outputFormat = TextOutputFormat.class;
        }
        MultipleOutputs.addNamedOutput(job, entry.getValue()+"", outputFormat, NullWritable.class, getHadoopType(rType.elementType()));
      }
    }
    /**
     * Define Reducer & Combiner
     */
    job.setCombinerClass(MSCRCombiner.class);
    job.setReducerClass(MSCRReducer.class);
    return job;
  }
  
  /**
   * Builds the execution tree out of a {@link PlumeWorkflow} by reading its class in hadoop configuration.
   * Then, it identifies which MSCR step is being executed by looking at the mscr_id parameter. Finally, it returns this MSCR.
   * 
   * @param conf
   * @return
   */
  static MSCR readMSCR(Configuration conf) {
    String className = conf.get(MapRedExecutor.WORKFLOW_NAME);
    int id = conf.getInt(MapRedExecutor.MSCR_ID, -1);
    if(id == -1) {
      throw new RuntimeException("No MSCR ID in hadoop conf.");
    }
    try {
      PlumeWorkflow workFlow = (PlumeWorkflow) Class.forName(className).newInstance();
      Optimizer optimizer = new Optimizer();
      ExecutionStep step = optimizer.optimize(workFlow);
      do {
      for(MSCR mscr: step.mscrSteps) {
        if(mscr.getId() == id) {
          return mscr;
        }
      }
      step = step.nextStep;
      } while(step != null);
      throw new RuntimeException("Invalid MSCR ID in hadoop conf: " + id);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * 
   * @param type
   * @return
   */
  static Class getHadoopType(PType type) {
    if(type instanceof BooleanType) {
      return BooleanWritable.class;
    }
    if(type instanceof DoubleType) {
      return DoubleWritable.class;
    }
    if(type instanceof FloatType) {
      return FloatWritable.class;
    }
    if(type instanceof IntegerType) {
      return IntWritable.class;
    }
    if(type instanceof LongType) {
      return LongWritable.class;
    }
    if(type instanceof StringType) {
      return Text.class;
    }
    if(type instanceof BytesType) {
      return BytesWritable.class;
    }
    throw new IllegalArgumentException("Unknown or unsupported PType type " + type);
  } 
}
