package com.tdunning.plume;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Jul 28, 2010 Time: 6:30:09 PM To change this
 * template use File | Settings | File Templates.
 */
public class Pair<K, V> {
  private K key;
  private V value;

  public Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public static <V1, V2> Pair<V1, V2> create(V1 x1, V2 x2) {
    return new Pair<V1, V2>(x1, x2);
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "Pair{" +
            "key=" + key +
            ", value=" + value +
            '}';
  }
}
