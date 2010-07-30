package com.tdunning.plume;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Jul 30, 2010 Time: 12:21:45 AM To change this
 * template use File | Settings | File Templates.
 */
public abstract class F3a<T, K, V> {
  public abstract void process(T value, KeyEmitter<K, V> emitter);
}
