package com.tdunning.plume;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Jul 29, 2010 Time: 10:24:09 PM To change this
 * template use File | Settings | File Templates.
 */
public abstract class F3<K, V, Vout> {
  public abstract void process(K key, V value, ValueEmitter<Vout> emitter);

}
