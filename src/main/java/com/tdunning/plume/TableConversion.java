package com.tdunning.plume;

/**
* Created by IntelliJ IDEA. User: tdunning Date: Jul 28, 2010 Time: 6:38:27 PM To change this
* template use File | Settings | File Templates.
*/
public abstract class TableConversion<X, K, V> {
  public abstract K key(X v);
  public abstract V value(X v);
}
