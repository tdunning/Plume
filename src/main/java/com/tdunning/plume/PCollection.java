package com.tdunning.plume;

/**
 * Parallel collection.
 */
public abstract class PCollection<T> implements Iterable<T> {
  public abstract <R>PCollection<R> map(DoFn<T, R> fn, CollectionConversion<R> conversion);
  public abstract <K, V>PTable<K, V> map(DoFn<T, Pair<K, V>> fn, TableConversion<K, V> conversion);

  // derived operations

  public abstract <K> PTable<K, Integer> count(PCollection<K> in);
}
