package com.tdunning.plume.local.lazy.op;

import java.util.List;

import com.tdunning.plume.PCollection;

/**
 * @param <T>
 */
public class Flatten<T> extends DeferredOp {

  List<PCollection<T>> origins;
  PCollection<T> dest;
  
  public Flatten(List<PCollection<T>> origins, PCollection<T> dest) {
    this.origins = origins;
    this.dest = dest;
  }
  
  public List<PCollection<T>> getOrigins() {
    return origins;
  }
  public PCollection<T> getDest() {
    return dest;
  }
}
