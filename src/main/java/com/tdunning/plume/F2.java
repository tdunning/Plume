package com.tdunning.plume;

/**
* Provides a skeleton for a map-like function.
*/
public abstract class F2<In,Out> {
  /**
   * Produces zero or more outputs from a single input.  The explicit passing of the emitter
   * might better be done by inheriting and emit method.  That style would allow us to provide
   * other capabilities to the process function.
   * @param x         The input.
   */
  public abstract void process(In x, ValueEmitter<Out> emitter);
}
