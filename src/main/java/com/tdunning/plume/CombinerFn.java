package com.tdunning.plume;

/**
* Created by IntelliJ IDEA. User: tdunning Date: Jul 29, 2010 Time: 10:33:34 PM To change this
* template use File | Settings | File Templates.
*/
public abstract class CombinerFn<T> {
  public abstract T combine(Iterable<T> stuff);
}
