package com.tdunning.plume;

import java.io.IOException;

/**
 * A plume provides the runtime support for writing data-parallel programs.  Each Plume implementation
 * defines a mode of execution.  For instance, the local.eager.LocalPlume provides instant execution
 * without execution planning or any parallel implementation.
 */
public abstract class Plume {
  // general collection operations
  public abstract PCollection<String> readTextFile(String name) throws IOException;
  public abstract PCollection<String> readResourceFile(String name) throws IOException;
  public abstract <T> PCollection<T> readAvroFile(String name, Conversion<T> format);
  public abstract <T> PCollection<T> fromJava(Iterable<T> source);
  public abstract <T> PCollection<T> flatten(PCollection<T>... args);

  // conversions that signal what kind of object we want
  public abstract <V> CollectionConversion<V> collectionOf(Encoding<V> valueEncoding);
  public abstract <V> CollectionConversion<V> sequenceOf(Encoding<V> valueEncoding);

  public abstract <R, K, V> TableConversion<R, K, V> tableOf(Class<K> keyClass, Class<V> valueClass);

  public class Conversion<T> {
  }
}
