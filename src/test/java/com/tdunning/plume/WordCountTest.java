package com.tdunning.plume;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.tdunning.plume.local.eager.LocalPlume;
import org.junit.Assert;
import org.junit.Test;

import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.util.Map;

import static com.tdunning.plume.Plume.*;

public class WordCountTest {
  @Test
  public void wordCount() throws IOException {
    final Splitter onNonWordChar = Splitter.on(CharMatcher.BREAKING_WHITESPACE);

    Plume p = new LocalPlume();
    
    PCollection<String> lines = p.readResourceFile("simple-text.txt");
    PCollection<String> words = lines.map(new DoFn<String, String>() {
      @Override
      public void process(String x, EmitFn<String> emitter) {
        for (String word : onNonWordChar.split(x)) {
          emitter.emit(word);
        }
      }
    }, collectionOf(strings()));

    PTable<String, Integer> wc = words
            .map(new DoFn<String, Pair<String, Integer>>() {
              @Override
              public void process(String x, EmitFn<Pair<String, Integer>> emitter) {
                emitter.emit(Pair.create(x, 1));
              }
            }, tableOf(strings(), integers()))
            .groupByKey()
            .combine(new CombinerFn<Integer>() {
              @Override
              public Integer combine(Iterable<Integer> counts) {
                int sum = 0;
                for (Integer k : counts) {
                  sum += k;
                }
                return sum;
              }
            });

    Map<String, Integer> m = Maps.newHashMap();
    for (Pair<String, Integer> pair : wc) {
      m.put(pair.getKey(), pair.getValue());
    }
    Assert.assertEquals(3, m.get("is").intValue());
    Assert.assertEquals(3, m.get("some").intValue());
    Assert.assertEquals(3, m.get("simple").intValue());
    Assert.assertEquals(1, m.get("examples").intValue());
    Assert.assertEquals(2, m.get("text").intValue());
  }

  @Test
  public void wordCountAvro() throws IOException {
    final Splitter onNonWordChar = Splitter.on(CharMatcher.BREAKING_WHITESPACE);

    Plume p = new LocalPlume();
    
    String file = Resources.getResource("simple-text.avro").getPath();
    PCollection<Utf8> lines = p.readAvroFile(file, strings());
    PCollection<String> words = lines.map(new DoFn<Utf8, String>() {
      @Override
      public void process(Utf8 x, EmitFn<String> emitter) {
        for (String word : onNonWordChar.split(x.toString())) {
          emitter.emit(word);
        }
      }
    }, collectionOf(strings()));

    PTable<String, Integer> wc = words.map(new DoFn<String, Pair<String, Integer>>() {
      @Override
      public void process(String x, EmitFn<Pair<String, Integer>> emitter) {
        emitter.emit(Pair.create(x, 1));
      }
    }, tableOf(strings(), integers()))
            .groupByKey()
            .combine(new CombinerFn<Integer>() {
              @Override
              public Integer combine(Iterable<Integer> counts) {
                int sum = 0;
                for (Integer k : counts) {
                  sum += k;
                }
                return sum;
              }
            });

    Map<String, Integer> m = Maps.newHashMap();
    for (Pair<String, Integer> pair : wc) {
      m.put(pair.getKey(), pair.getValue());
    }
    Assert.assertEquals(3, m.get("is").intValue());
    Assert.assertEquals(3, m.get("some").intValue());
    Assert.assertEquals(3, m.get("simple").intValue());
    Assert.assertEquals(1, m.get("examples").intValue());
    Assert.assertEquals(2, m.get("text").intValue());
  }

}
