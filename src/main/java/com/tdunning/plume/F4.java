package com.tdunning.plume;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Jul 29, 2010 Time: 11:46:16 PM To change this
 * template use File | Settings | File Templates.
 */
public abstract class F4<K, V, K1, V1> {
  public abstract void process(K key, V value, KeyEmitter<K1, V1> emitter);
}
