package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.DoFn;

public class ParallelDo<T, V> extends DeferredOp {

  DoFn<T, V> function;

  public ParallelDo(DoFn<T, V> function) {
    super();
    this.function = function;
  }

  public DoFn<T, V> getFunction() {
    return function;
  }
}
