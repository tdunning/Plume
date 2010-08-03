package com.tdunning.plume.local.lazy.op;

import java.util.List;

import com.tdunning.plume.PTable;

/**
 * @param <K>
 * @param <V>
 */
public class FlattenT<K, V> {

  List<PTable<K, V>> tables;
  PTable<K, V> dest;
  
  public FlattenT(List<PTable<K, V>> tables, PTable<K, V> dest) {
    super();
    this.tables = tables;
    this.dest = dest;
  }

  public List<PTable<K, V>> getTables() {
    return tables;
  }
  public PTable<K, V> getDest() {
    return dest;
  }
}
