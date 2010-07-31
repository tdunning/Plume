package com.tdunning.plume;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Jul 31, 2010 Time: 2:29:43 PM To change this
 * template use File | Settings | File Templates.
 */
public abstract class EmitFn<Out> {
  public abstract void emit(Out v);
}
