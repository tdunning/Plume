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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Pair;
import com.tdunning.plume.local.lazy.LazyCollection;
import com.tdunning.plume.local.lazy.MSCR;
import com.tdunning.plume.local.lazy.MSCRMapRedBase;
import com.tdunning.plume.local.lazy.MSCRToMapRed.PlumeObject;
import com.tdunning.plume.local.lazy.op.DeferredOp;
import com.tdunning.plume.local.lazy.op.Flatten;
import com.tdunning.plume.local.lazy.op.GroupByKey;
import com.tdunning.plume.local.lazy.op.MultipleParallelDo;
import com.tdunning.plume.types.PCollectionType;
import com.tdunning.plume.types.PTableType;

/**
 * Mapper that is used to execute MSCR in MapReds - Work-in-progress.
 */
public class MSCRMapper extends Mapper<WritableComparable, WritableComparable, PlumeObject, PlumeObject>  {
  
  MSCR mscr;
  
  protected void setup(Mapper<WritableComparable, WritableComparable, PlumeObject, PlumeObject>.Context context) 
    throws IOException, InterruptedException {
  
    this.mscr = MSCRMapRedBase.readMSCR(context.getConfiguration());
  };

  @SuppressWarnings("unchecked")
  protected void map(WritableComparable key, WritableComparable value, 
      final Mapper<WritableComparable, WritableComparable, PlumeObject, PlumeObject>.Context context) 
    throws IOException, InterruptedException {
  
    LazyCollection<?> l = null;

    // This class is private!
    TaggedInputSplit t = (TaggedInputSplit)context.getInputSplit();
    FileSplit fS = (FileSplit)t.getInputSplit();
    
    // Get LazyCollection for this input (according to FileSplit)
    for(PCollection<?> input: mscr.getInputs()) {
      LazyCollection<?> thisL = (LazyCollection<?>)input;
      if(thisL.getFile().equals(fS.getPath().toString()) ||
          ("file:" + thisL.getFile()).equals(fS.getPath().toString())) {
        l = thisL;
        break;
      }
    }
    
    // If this collection is a table -> process Pair, otherwise process value
    PCollectionType type = l.getType();
    Object toProcess = value;
    if(type instanceof PTableType) {
      toProcess = Pair.create(key, value);
    }

    DeferredOp op = l.getDownOps().get(0); // WARN assuming only one op can follow mscr input collections (after optimizing)
    if(op instanceof MultipleParallelDo) {
      MultipleParallelDo mPDo = ((MultipleParallelDo)op);
      for(Object entry: mPDo.getDests().entrySet()) {
        Map.Entry<PCollection, DoFn> en = (Map.Entry<PCollection, DoFn>)entry;
        LazyCollection<?> lCol = (LazyCollection<?>)en.getKey();
        op = lCol.getDownOps().get(0); // same here
        final int channel;
        if(op instanceof Flatten) {
          LazyCollection col = (LazyCollection)((Flatten)op).getDest();
          GroupByKey gBK = (GroupByKey)col.getDownOps().get(0); 
          channel = mscr.getNumberedChannels().get(gBK);
        } else if(op instanceof GroupByKey) {
          channel = mscr.getNumberedChannels().get((GroupByKey)op);
        } else {
          throw new RuntimeException("Invalid MSCR");
        }
        // Call parallelDo function
        en.getValue().process(toProcess, new EmitFn() {
          @Override
          public void emit(Object v) {
            Pair p = (Pair)v; // TODO how to report this. Same with WritableComparable type safety.
            try {
              context.write(
                new PlumeObject((WritableComparable)p.getKey(), channel),
                new PlumeObject((WritableComparable)p.getValue(), channel)
              );
            } catch (Exception e) {
              e.printStackTrace(); // TODO How to report this
            }
          }
        });
      }
    } else if(op instanceof Flatten) {
      LazyCollection col = (LazyCollection)((Flatten)op).getDest();
      GroupByKey gBK = (GroupByKey)col.getDownOps().get(0); // TODO Report these possible exceptions as malformed MSCR
      int channel = mscr.getNumberedChannels().get(gBK);
      context.write(new PlumeObject(key, channel), new PlumeObject(value, channel));
    } else if(op instanceof GroupByKey) {
      int channel = mscr.getNumberedChannels().get((GroupByKey)op);
      context.write(new PlumeObject(key, channel), new PlumeObject(value, channel));
    } 
  };  
}