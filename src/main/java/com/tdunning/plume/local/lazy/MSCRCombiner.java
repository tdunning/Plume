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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Lists;
import com.tdunning.plume.local.lazy.MSCR.OutputChannel;
import com.tdunning.plume.local.lazy.MSCRToMapRed.PlumeObject;
import com.tdunning.plume.local.lazy.op.GroupByKey;

/**
 * Combiner that is used to executed MSCR in Map/reds - Work in progress
 */
public class MSCRCombiner extends MSCRMapRedBase implements Reducer<PlumeObject, PlumeObject, PlumeObject, PlumeObject> {

  @Override
  public void configure(JobConf arg0) {
    readMSCR(arg0);
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void reduce(PlumeObject arg0, Iterator<PlumeObject> arg1,
      OutputCollector<PlumeObject, PlumeObject> arg2, Reporter arg3)
      throws IOException {

    GroupByKey gBK = mscr.getChannelByNumber().get(arg0.sourceId);
    OutputChannel oC = mscr.getOutputChannels().get(gBK);
    if(oC.combiner != null) {
      // Apply combiner function for this channel
      List<WritableComparable> vals = Lists.newArrayList();
      while(arg1.hasNext()) {
        vals.add(arg1.next().obj);
      }
      WritableComparable result = (WritableComparable) oC.combiner.getCombiner().combine(vals);
      arg2.collect(arg0, new PlumeObject(result, arg0.sourceId));
    } else {
      // direct writing - write all key, value pairs
      while(arg1.hasNext()) {
        arg2.collect(arg0, arg1.next());
      }
    }
  }
}
