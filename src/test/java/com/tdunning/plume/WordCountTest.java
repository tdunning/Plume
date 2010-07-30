package com.tdunning.plume;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.tdunning.plume.local.eager.LocalPlume;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: Jul 29, 2010 Time: 9:31:53 PM To change this
 * template use File | Settings | File Templates.
 */
public class WordCountTest {
  @Test
  public void wordCount() throws IOException {
    final Splitter onNonWordChar = Splitter.on(CharMatcher.BREAKING_WHITESPACE);
    Plume p = new LocalPlume();

    PCollection<String> lines = p.readResourceFile("simple-text.txt");
    PCollection<String> words = lines.map(new F2<String, String>() {
      @Override
      public void process(String x, ValueEmitter<String> emitter) {
        for (String word : onNonWordChar.split(x)) {
          emitter.emit(word);
        }
      }
    });

    PTable<String, Integer> wc = words.map(new F3a<String, String, Integer>() {
      @Override
      public void process(String x, KeyEmitter<String, Integer> emitter) {
        emitter.emit(x, 1);
      }
    })
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
