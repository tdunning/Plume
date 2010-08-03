package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.DoFn;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;

public class ParallelDoTT<K, V, K1, V1> extends ParallelDo<Pair<K, V>, Pair<K1, V1>> {

  PTable<K, V> origin;
  PTable<K1, V1> dest;
  
  public ParallelDoTT(PTable<K, V> origin, PTable<K1, V1> dest,
      DoFn<Pair<K, V>, Pair<K1, V1>> function) {
    super(function);
    this.origin = origin;
    this.dest = dest;
  }
  
  public PTable<K, V> getOrigin() {
    return origin;
  }
  public PTable<K1, V1> getDest() {
    return dest;
  }
  public DoFn<Pair<K, V>, Pair<K1, V1>> getFunction() {
    return function;
  }
}
