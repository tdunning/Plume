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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.local.lazy.MSCR.OutputChannel;
import com.tdunning.plume.local.lazy.MapRedExecutor.PlumeObject;
import com.tdunning.plume.local.lazy.op.GroupByKey;

/**
 * Combiner that is used to executed MSCR in Map/reds
 */
public class MSCRCombiner extends Reducer<PlumeObject, PlumeObject, PlumeObject, PlumeObject> {

  MSCR mscr; // Current MSCR being executed
  
  protected void setup(Reducer<PlumeObject, PlumeObject, PlumeObject, PlumeObject>.Context context)
    throws IOException, InterruptedException {

    this.mscr = MapRedExecutor.readMSCR(context.getConfiguration());
  }
  
  @SuppressWarnings("unchecked")
  protected void reduce(final PlumeObject arg0, java.lang.Iterable<PlumeObject> values,
      Reducer<PlumeObject, PlumeObject, PlumeObject, PlumeObject>.Context context)
    throws IOException, InterruptedException {

    PCollection col  = mscr.getChannelByNumber().get(arg0.sourceId);
    OutputChannel oC = mscr.getOutputChannels().get(col);
    if(oC.combiner != null) {
      // Apply combiner function for this channel
      List<WritableComparable> vals = Lists.newArrayList();
      for(PlumeObject val: values) {
        vals.add(val.obj);
      }
      WritableComparable result = (WritableComparable) oC.combiner.getCombiner().combine(vals);
      context.write(arg0, new PlumeObject(result, arg0.sourceId));
    } else {
      // direct writing - write all key, value pairs
      for(PlumeObject val: values) {
        context.write(arg0, val);
      }
    }
  }
}
