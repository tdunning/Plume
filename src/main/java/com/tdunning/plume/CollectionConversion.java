package com.tdunning.plume;

/**
* Signals to map that the result should be a PCollection rather than a PTable.  By using
 * arguments with different types, we can signal to the type system what kind of result
 * we want to get as a result.
*/
public abstract class CollectionConversion<R> {
}
