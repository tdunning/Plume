package com.tdunning.plume;

/**
 * Describes the key functional object that processes an input record and
 * outputs zero or more objects by means of an emitter object.
 */
public abstract class DoFn<In, Out> {
  public abstract void process(In v, EmitFn<Out> emitter);
}
