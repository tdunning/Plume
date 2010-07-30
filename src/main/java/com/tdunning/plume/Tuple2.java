package com.tdunning.plume;

import java.util.Set;

/**
* Created by IntelliJ IDEA. User: tdunning Date: Jul 29, 2010 Time: 6:26:41 PM To change this
* template use File | Settings | File Templates.
*/
public class Tuple2<V0, V1> {
  private V0 v0;
  private V1 v1;

  public Tuple2(V0 v0, V1 v1) {
    //To change body of created methods use File | Settings | File Templates.
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
