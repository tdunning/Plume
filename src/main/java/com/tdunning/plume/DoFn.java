package com.tdunning.plume;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Jul 31, 2010 Time: 2:28:25 PM To change this
 * template use File | Settings | File Templates.
 */
public abstract class DoFn<In, Out> {
  public abstract void process(In v, EmitFn<Out> emitter);
}
