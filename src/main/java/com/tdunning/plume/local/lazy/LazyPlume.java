package com.tdunning.plume.local.lazy;

import java.io.IOException;

import com.google.common.collect.Lists;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.Plume;
import com.tdunning.plume.local.lazy.op.Flatten;

/**
 * Runtime for Plume implementing deferred execution and optimization.
 * 
 * @author pere
 *
 */
public class LazyPlume extends Plume {

  @Override
  public PCollection<String> readTextFile(String name) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PCollection<String> readResourceFile(String name) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> PCollection<T> readAvroFile(String name, Class<T> targetClass) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> PCollection<T> fromJava(Iterable<T> source) {
    return new LazyCollection<T>(source);
  }

  @Override
  public <T> PCollection<T> flatten(PCollection<T>... args) {
    LazyCollection<T> dest = new LazyCollection<T>();
    Flatten<T> flatten = new Flatten<T>(Lists.newArrayList(args), dest);
    dest.deferredOp = flatten;
    for(PCollection<T> col: args) {
      ((LazyCollection<T>)col).addDownOp(flatten);
    }
    return dest;
  }
}
