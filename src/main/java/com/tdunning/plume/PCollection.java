package com.tdunning.plume;

/**
 * Parallel collection.
 */
public abstract class PCollection<T> implements Iterable<T> {
  public abstract <R>PCollection<R> map(F2<T, R> fn);
  public abstract <K, V>PTable<K, V> map(F3a<T, K, V> fn);

  // derived operations

  public abstract <K> PTable<K, Integer> count(PCollection<K> in);
}
