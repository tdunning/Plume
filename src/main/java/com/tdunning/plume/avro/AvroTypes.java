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

package com.tdunning.plume.avro;

import com.tdunning.plume.types.*;
import com.tdunning.plume.types.PType.Kind;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.Pair;

/**
 * Translate between Plume types to Avro types.
 */
public class AvroTypes {
  // no public ctor
  private AvroTypes() {
  }

  /**
   * Convert Plume types to Avro types.
   */
  public static Schema getSchema(PType type) {
    switch (type.kind()) {
      case BOOLEAN:
        return Schema.create(Type.BOOLEAN);
      case BYTES:
        return Schema.create(Type.BYTES);
      case DOUBLE:
        return Schema.create(Type.DOUBLE);
      case FLOAT:
        return Schema.create(Type.FLOAT);
      case INTEGER:
        return Schema.create(Type.INT);
      case LONG:
        return Schema.create(Type.LONG);
      case PAIR:
        PairType pairType = (PairType) type;
        return Pair.getPairSchema(
                getSchema(pairType.keyType()),
                getSchema(pairType.valueType()));
      case COLLECTION:
        PType elementType = ((PCollectionType) type).elementType();
        return Schema.createArray(getSchema(elementType));
      case TABLE:
        PType keyType = ((PTableType) type).keyType();
        PType valueType = ((PTableType) type).valueType();
        // PTable<String,*> is an Avro map
        if (keyType.kind() == Kind.STRING) {
          return Schema.createMap(getSchema(new PairType(keyType, valueType)));
        }
        return Schema.createArray(getSchema(keyType));
      case RECORD:
        return ((RecordType) type).schema();
      case STRING:
        return Schema.create(Type.STRING);
      default:
        throw new RuntimeException("Unknown type: " + type);
    }
  }

  /**
   * Convert Avro types to Plume types.
   */
  public static PType getPType(Schema schema) {
    // TODO FIXME
    throw new RuntimeException("Not yet implemented.");
  }

}
