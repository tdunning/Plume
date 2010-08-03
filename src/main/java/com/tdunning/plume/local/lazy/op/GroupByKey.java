package com.tdunning.plume.local.lazy.op;

import com.tdunning.plume.local.lazy.LazyTable;

/**
 * 
 * @author pere
 * 
 * @param <K>
 * @param <V>
 */
public class GroupByKey<K, V> extends DeferredOp {

  LazyTable<K, V> origin;
  LazyTable<K, Iterable<V>> dest;

  public GroupByKey(LazyTable<K, V> origin, LazyTable<K, Iterable<V>> dest) {
    this.origin = origin;
    this.dest = dest;
  }

  public LazyTable<K, Iterable<V>> getDest() {
    return dest;
  }

  public LazyTable<K, V> getOrigin() {
    return origin;
  }
}