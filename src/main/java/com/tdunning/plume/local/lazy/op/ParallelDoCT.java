package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.DoFn;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;

public class ParallelDoCT<T, K, V> extends ParallelDo<T, Pair<K, V>> {

  PCollection<T> origin;
  PTable<K, V> dest;
  
  public ParallelDoCT(PCollection<T> origin, PTable<K, V> dest,
      DoFn<T, Pair<K, V>> function) {
    super(function);
    this.origin = origin;
    this.dest = dest;
  }
  
  public PCollection<T> getOrigin() {
    return origin;
  }
  public PTable<K, V> getDest() {
    return dest;
  }
  public DoFn<T, Pair<K, V>> getFunction() {
    return function;
  }
}
