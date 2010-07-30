package com.tdunning.plume;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Jul 29, 2010 Time: 12:53:16 PM To change this
 * template use File | Settings | File Templates.
 */
public class BasicEmitter<Out> extends ValueEmitter<Out> {
  @Override
  public void emit(Out y) {
    throw new UnsupportedOperationException("Can't emit without key");
  }
}
