package com.tdunning.plume;

/**
 * Describes the interface for an associative aggregation function that can be applied one or more
 * times.  Since this function is associative, it can be used as a combiner in addition to being
 * part of reducer.
 */
public abstract class CombinerFn<T> {
  public abstract T combine(Iterable<T> stuff);
}
