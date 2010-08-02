/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
