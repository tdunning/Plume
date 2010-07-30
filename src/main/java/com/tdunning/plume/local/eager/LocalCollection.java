package com.tdunning.plume.local.eager;

import com.google.common.collect.Lists;
import com.tdunning.plume.*;

import java.util.Iterator;
import java.util.List;

/**
* Completely local version of a PCollection.
*/
public class LocalCollection<T> extends PCollection<T> {
  private List<T> data = Lists.newArrayList();

  @Override
  public <R> PCollection<R> map(F2<T, R> fn) {
    final LocalCollection<R> r = new LocalCollection<R>();
    for (T t : data) {
      fn.process(t, new ValueEmitter<R>() {
        @Override
        public void emit(R y) {
          r.data.add(y);
        }
      });
    }
    return r;
  }

  @Override
  public <K, V> PTable<K, V> map(F3a<T, K, V> fn) {
    final LocalTable<K, V> r = new LocalTable<K, V>();
    for (final T t : data) {
      fn.process(t, new KeyEmitter<K, V>() {
        @Override
        public void emit(K key, V value) {
          r.getData().add(Pair.create(key, value));
        }
      });
    }
    return r;
  }

  @Override
  public <K> PTable<K, Integer> count(PCollection<K> in) {
    // TODO implement count in PCollection
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static <X> LocalCollection<X> wrap(Iterable<X> data) {
    return new LocalCollection<X>().addAll(data);
  }

  public LocalCollection<T> addAll(Iterable<T> data) {
    this.data = Lists.newArrayList(data);
    return this;
  }

  public List<T> getData() {
    return data;
  }

  /**
   * Returns an iterator over a set of elements of type T.
   *
   * @return an Iterator.
   */
  public Iterator<T> iterator() {
    return data.iterator();
  }
}
