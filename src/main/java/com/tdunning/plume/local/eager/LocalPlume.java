package com.tdunning.plume.local.eager;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.tdunning.plume.*;
import com.tdunning.plume.local.eager.LocalCollection;

import java.io.File;
import java.io.IOException;

/**
 * Local plume runtime.  All files are local, all tasks are run in threads.  Currently threads == thread
 */
public class LocalPlume extends Plume {
  @Override
  public LocalCollection<String> readTextFile(String name) throws IOException {
    return LocalCollection.wrap(Files.readLines(new File(name), Charsets.UTF_8));
  }

  @Override
  public PCollection<String> readResourceFile(String name) throws IOException {
    return LocalCollection.wrap(Resources.readLines(Resources.getResource(name), Charsets.UTF_8));
  }

  @Override
  public <T> PCollection<T> readAvroFile(String name, Plume.Conversion<T> format) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <X> LocalCollection<X> fromJava(Iterable<X> data) {
    return new LocalCollection<X>().addAll(data);
  }

  @Override
  public <T> PCollection<T> flatten(PCollection<T>... args) {
    LocalCollection<T> r = new LocalCollection<T>();
    for (PCollection<T> arg : args) {
      if (arg instanceof LocalCollection) {
        r.addAll(((LocalCollection<T>) arg).getData());
      }
    }
    return r;
  }

  @Override
  public <V> CollectionConversion<V> collectionOf(Encoding<V> valueClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <V> CollectionConversion<V> sequenceOf(Encoding<V> valueClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <R, K, V> TableConversion<R, K, V> tableOf(Class<K> keyClass, Class<V> valueClass) {
    return null;
  }
}
