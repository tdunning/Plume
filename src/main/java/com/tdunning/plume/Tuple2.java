package com.tdunning.plume;

/**
 * Container for two objects used during joins.  It is an open question whether a Pair should just
 * be a Tuple2 (or vice versa).
 */
public class Tuple2<V0, V1> {
  private V0 v0;
  private V1 v1;

  private Tuple2(V0 v0, V1 v1) {
    this.v0 = v0;
    this.v1 = v1;
  }

  public static <V0, V1> Tuple2<V0, V1> create(V0 v0, V1 v1) {
    return new Tuple2<V0, V1>(v0, v1);
  }

  public V0 get0() {
    return v0;
  }

  public V1 get1() {
    return v1;
  }
}
