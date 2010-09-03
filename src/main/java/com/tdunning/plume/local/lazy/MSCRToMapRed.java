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
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import com.tdunning.plume.PCollection;
import com.tdunning.plume.local.lazy.MSCR.OutputChannel;
import com.tdunning.plume.local.lazy.op.GroupByKey;
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
 * This class converts a MSCR into an executable MapRed job. - Work-in-progress
 */
public class MSCRToMapRed {

  public final static String WORKFLOW_NAME = "plume.workflow.name"; // hadoop conf. property used to instantiate proper workflow
  public final static String MSCR_ID = "plume.workflow.mscr.id"; // TODO in the future, will identify current MSCR being executed

  /**
   * Wrapper class for multi-type shuffling
   *
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static class PlumeObject implements WritableComparable<PlumeObject> {

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
  
  @SuppressWarnings({ "deprecation", "unchecked" })
  public static JobConf getMapRed(final MSCR mscr, PlumeWorkflow workflow, String jobId, String outputPath) {
    Configuration conf = new Configuration();

    JobConf job = new JobConf(conf, MSCRToMapRed.class);
    job.setJobName("MSCR " + jobId);

    job.setMapOutputKeyClass(PlumeObject.class);
    job.setMapOutputValueClass(PlumeObject.class);

    job.setJarByClass(MSCRToMapRed.class);

    job.set(WORKFLOW_NAME, workflow.getClass().getName());
    job.set(MSCR_ID, mscr.getId()+"");
    
    /**
     * Inputs
     */
    for(PCollection<?> input: mscr.getInputs()) {
      if(!(input instanceof LazyCollection)) {
        throw new IllegalArgumentException("Can't create MapRed from MSCR whose inputs are not LazyTable");
      }
      LazyCollection<Text> l = (LazyCollection<Text>)input;
      if(!(l.isMaterialized() && l.getFile() != null)) {
        throw new IllegalArgumentException("Can't create MapRed from MSCR inputs that are not materialized to a file");
      }
      PCollectionType rType = l.getType();
      Class<? extends InputFormat> format =  SequenceFileInputFormat.class;
      if(rType instanceof PTableType) {
        PTableType tType = (PTableType)rType;
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
    for(Map.Entry<GroupByKey<?, ?>, Integer> entry: mscr.getNumberedChannels().entrySet()) {
      OutputChannel<?, ?, ?> oC = mscr.getOutputChannels().get(entry.getKey());
      PCollectionType rType = entry.getKey().getDest().getType();
      if(oC.reducer != null) {
        rType = ((LazyCollection)oC.reducer.getDest()).getType();
      }
      if(rType instanceof PTableType) {
        PTableType tType = (PTableType)rType;
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
   * 
   * @param type
   * @return
   */
  public static Class getHadoopType(PType type) {
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
