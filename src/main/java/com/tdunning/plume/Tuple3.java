package com.tdunning.plume;

/**
* Created by IntelliJ IDEA. User: tdunning Date: Jul 29, 2010 Time: 6:26:41 PM To change this
* template use File | Settings | File Templates.
*/
public class Tuple3<V1,V2, V3> {
  public Tuple3(V1 v1, V2 v2, V3 v3) {
    //To change body of created methods use File | Settings | File Templates.
  }

  public static <V1, V2, V3> Tuple3<V1, V2, V3> create(V1 v1, V2 v2, V3 v3) {
    return new Tuple3<V1, V2, V3>(v1, v2, v3);
  }
}
