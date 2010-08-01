package com.tdunning.plume;

/**
* A triple of three objects.  
*/
public class Tuple3<V1,V2, V3> {
  public Tuple3(V1 v1, V2 v2, V3 v3) {
    //To change body of created methods use File | Settings | File Templates.
  }

  public static <V1, V2, V3> Tuple3<V1, V2, V3> create(V1 v1, V2 v2, V3 v3) {
    return new Tuple3<V1, V2, V3>(v1, v2, v3);
  }
}
