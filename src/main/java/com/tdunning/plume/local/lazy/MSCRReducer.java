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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.MSCR.OutputChannel;
import com.tdunning.plume.local.lazy.MSCRToMapRed.PlumeObject;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.ParallelDo;

/**
 * Reducer that is used to execute MSCR in MapReds - Work-in-progress
 */
public class MSCRReducer extends MSCRMapRedBase implements Reducer<PlumeObject, PlumeObject, NullWritable, NullWritable> {

  MultipleOutputs mos;
  
  @Override
  public void configure(JobConf arg0) {
    mos = new MultipleOutputs(arg0);
    readMSCR(arg0);
  }
  
  @Override
  public void close() throws IOException {
    mos.close();
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void reduce(final PlumeObject arg0, Iterator<PlumeObject> values,
      OutputCollector<NullWritable, NullWritable> arg2, final Reporter arg3)
  throws IOException {

    GroupByKey gBK = mscr.getChannelByNumber().get(arg0.sourceId);
    OutputChannel oC = mscr.getOutputChannels().get(gBK);
    if(oC.reducer != null) {
      // apply reducer
      ParallelDo pDo = oC.reducer;
      DoFn reducer = pDo.getFunction(); // TODO how to check / report this
      List<WritableComparable> vals = Lists.newArrayList();
      while(values.hasNext()) {
        vals.add(values.next().obj);
      }
      reducer.process(Pair.create(arg0.obj, vals), new EmitFn() {
      @Override
        public void emit(Object v) {
          try {
            if(v instanceof Pair) {
              Pair p = (Pair)v; 
              mos.getCollector(arg0.sourceId+"", arg3).collect(p.getKey(), p.getValue());
            } else {
              mos.getCollector(arg0.sourceId+"", arg3).collect(NullWritable.get(), (WritableComparable)v);
            }
          } catch (IOException e) {
            e.printStackTrace(); // TODO How to report this
          }
        }
      });
    } else {
      // direct writing - write all key, value pairs
      while(values.hasNext()) {
        mos.getCollector(arg0.sourceId+"", arg3).collect(arg0.obj, values.next().obj);
      }
    }
  }
}