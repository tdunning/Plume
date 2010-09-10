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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.MSCR.OutputChannel;
import com.tdunning.plume.local.lazy.MapRedExecutor.PlumeObject;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.ParallelDo;

/**
 * Reducer that is used to execute MSCR in MapReds
 */
public class MSCRReducer extends Reducer<PlumeObject, PlumeObject, NullWritable, NullWritable> {

  MultipleOutputs mos;
  MSCR mscr; // Current MSCR being executed
  
  protected void setup(Reducer<PlumeObject, PlumeObject, NullWritable, NullWritable>.Context context)
    throws IOException, InterruptedException {

    this.mos  = new MultipleOutputs(context);
    this.mscr = MapRedExecutor.readMSCR(context.getConfiguration());
  }
  
  protected void cleanup(Reducer<PlumeObject, PlumeObject, NullWritable, NullWritable>.Context context) 
    throws IOException ,InterruptedException {
    
    mos.close();
  }
  
  @SuppressWarnings("unchecked")
  protected void reduce(final PlumeObject arg0, java.lang.Iterable<PlumeObject> values,
      Reducer<PlumeObject,PlumeObject,NullWritable,NullWritable>.Context arg2)
    throws IOException, InterruptedException {
    
    PCollection col  = mscr.getChannelByNumber().get(arg0.sourceId);
    OutputChannel oC = mscr.getOutputChannels().get(col);
    if(oC.reducer != null) {
      // apply reducer
      ParallelDo pDo = oC.reducer;
      DoFn reducer = pDo.getFunction(); // TODO how to check / report this
      List<WritableComparable> vals = Lists.newArrayList();
      for(PlumeObject val: values) {
        vals.add(val.obj);
      }
      reducer.process(Pair.create(arg0.obj, vals), new EmitFn() {
      @Override
        public void emit(Object v) {
          try {
            if(v instanceof Pair) {
              Pair p = (Pair)v; 
              mos.write(arg0.sourceId+"", p.getKey(), p.getValue());
            } else {
              mos.write(arg0.sourceId+"", NullWritable.get(), (WritableComparable)v);
            }
          } catch (Exception e) {
            e.printStackTrace(); // TODO How to report this
          }
        }
      });
    } else {
      // direct writing - write all key, value pairs
      for(PlumeObject val: values) {
        mos.write(arg0.sourceId+"", arg0.obj, val.obj);
      }
    }
  }
}