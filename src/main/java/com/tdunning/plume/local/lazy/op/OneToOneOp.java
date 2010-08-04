package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.PCollection;

public abstract class OneToOneOp<T, V> extends DeferredOp {

  public abstract PCollection<T> getOrigin();
  public abstract PCollection<V> getDest();
}
