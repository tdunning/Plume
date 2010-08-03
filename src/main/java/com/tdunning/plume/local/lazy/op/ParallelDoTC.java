package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.DoFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;

public class ParallelDoTC<K, V, T> extends ParallelDo<Pair<K, V>, T> {

  PTable<K, V> origin;
  PCollection<T> dest;

  public ParallelDoTC(PTable<K, V> origin, PCollection<T> dest,
      DoFn<Pair<K, V>, T> function) {
    super(function);
    this.origin = origin;
    this.dest = dest;
  }
  
  public PTable<K, V> getOrigin() {
    return origin;
  }
  public PCollection<T> getDest() {
    return dest;
  }
  public DoFn<Pair<K, V>, T> getFunction() {
    return function;
  }
}
