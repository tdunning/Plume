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

package com.tdunning.plume.local;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.Ordering;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.Plume;
import com.tdunning.plume.local.eager.LocalPlume;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static com.tdunning.plume.Plume.strings;

public class LogParseTest {
  @Test
  public void parseGroupSort() throws IOException {
    Plume p = new LocalPlume();
    PCollection<String> logs = p.readResourceFile("log.txt");
    PTable<String, Event> events = logs.map(new DoFn<String, Pair<String, Event>>() {
      @Override
      public void process(String logLine, EmitFn<Pair<String, Event>> emitter) {
        Event e = new Event(logLine);
        emitter.emit(new Pair<String, Event>(e.getName(), e));
      }
    }, Plume.tableOf(strings(), strings()));

    PTable<String, Iterable<Event>> byName = events.groupByKey(new Ordering<Event>() {
      // what goes here??
    });
  }

  private static final class Event implements Comparable<Event> {
    private static final Splitter onWhiteSpace = Splitter.on(CharMatcher.BREAKING_WHITESPACE);
    private String time;
    private String name;
    private String msg;

    public Event(String logLine) {
      Iterator<String> pieces = onWhiteSpace.split(logLine).iterator();
      time = pieces.next();
      name = pieces.next();
      msg = pieces.next();
    }

    public String getName() {
      return name;
    }

    @Override
    public int compareTo(Event o) {
      return this.time.compareTo(o.time);
    }
  }
}
