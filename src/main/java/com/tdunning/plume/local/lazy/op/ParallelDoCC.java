package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.DoFn;
import com.tdunning.plume.PCollection;

/**
 * 
 * @author pere
 */
public class ParallelDoCC<T, V> extends ParallelDo<T, V> {

  PCollection<T> origin;
  PCollection<V> dest;

  public ParallelDoCC(PCollection<T> origin, PCollection<V> dest,
      DoFn<T, V> function) {
    super(function);
    this.origin = origin;
    this.dest = dest;
  }

  public DoFn<T, V> getFunction() {
    return function;
  }

  public PCollection<T> getOrigin() {
    return origin;
  }

  public PCollection<V> getDest() {
    return dest;
  }
}