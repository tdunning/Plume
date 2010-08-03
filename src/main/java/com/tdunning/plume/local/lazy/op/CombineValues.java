package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.CombinerFn;
import com.tdunning.plume.PTable;

/**
 * 
 * @author pere
 * 
 */
public class CombineValues<K, V> extends DeferredOp {

  PTable<K, Iterable<V>> origin;
  PTable<K, V> dest;
  CombinerFn<V> combiner; 
  
  public CombineValues(CombinerFn<V> combiner, PTable<K, V> dest, PTable<K, Iterable<V>> origin) {
    this.origin = origin;
    this.combiner = combiner;
    this.dest = dest;
  }

  public PTable<K, Iterable<V>> getOrigin() {
    return origin;
  }

  public PTable<K, V> getDest() {
    return dest;
  }

  public CombinerFn<V> getCombiner() {
    return combiner;
  }
}
