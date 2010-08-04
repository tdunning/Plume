package com.tdunning.plume;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.tdunning.plume.local.eager.LocalPlume;
import org.junit.Test;

import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.util.Map;

import static com.tdunning.plume.Plume.*;
import static org.junit.Assert.assertEquals;

public class WordCountTest {
  @Test
  public void wordCount() throws IOException {

    Plume p = new LocalPlume();

    PCollection<String> lines = p.readResourceFile("simple-text.txt");
    countWords(lines);
  }

  @Test
  public void wordCountAvro() throws IOException {

    Plume p = new LocalPlume();
    
    String file = Resources.getResource("simple-text.avro").getPath();
    PCollection<CharSequence> lines = p.readAvroFile(file, strings())
            .map(new DoFn<CharSequence, CharSequence>() {
              @Override
              public void process(CharSequence x, EmitFn<CharSequence> emitter) {
                emitter.emit(x);
              }
            }, collectionOf(strings()));

    countWords(lines);
  }

  private <T extends CharSequence> void countWords(PCollection<T> lines) {
    final Splitter onNonWordChar = Splitter.on(CharMatcher.BREAKING_WHITESPACE);

    PCollection<String> words = lines.map(new DoFn<T, String>() {
      @Override
      public void process(T x, EmitFn<String> emitter) {
        for (String word : onNonWordChar.split(x.toString())) {
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
    assertEquals(3, m.get("is").intValue());
    assertEquals(3, m.get("some").intValue());
    assertEquals(3, m.get("simple").intValue());
    assertEquals(1, m.get("examples").intValue());
    assertEquals(2, m.get("text").intValue());
  }
}
