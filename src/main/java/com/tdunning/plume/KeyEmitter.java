package com.tdunning.plume;

/**
* Created by IntelliJ IDEA. User: tdunning Date: Jul 29, 2010 Time: 10:46:18 PM To change this
* template use File | Settings | File Templates.
*/
public abstract class KeyEmitter<K, V> {
  public abstract void emit(K key, V value);
}
