package com.tdunning.plume;

/**
 * Describes the interface for an object used to emit results from a DoFn.
 */
public abstract class EmitFn<Out> {
  public abstract void emit(Out v);
}
