/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package storm.benchmark.lib.bolt;

import org.apache.storm.Config;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.benchmark.lib.reducer.LongSummer;
import storm.benchmark.tools.SlidingWindow;
import storm.benchmark.metrics.LatencyConsumer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.lang.NullPointerException;
import storm.benchmark.metrics.Latencies;
import redis.clients.jedis.Jedis;
/**
 * forked from RollingCountBolt in storm-starter
 */

public class RollingCountBolt extends RollingBolt {

  private static final long serialVersionUID = -903093673694769540L;
  private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);
  public static final String FIELDS_OBJ = "obj";
  public static final String FIELDS_CNT = "count";
  public static final String TIMESTAMP = "timestamp";
 
  static transient Jedis jedis;
  private final SlidingWindow<Object, Long> window;
  transient Latencies _latencies;
   
  public RollingCountBolt() {
    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }

  public RollingCountBolt(int winLen, int emitFreq) {
    super(winLen, emitFreq);
    window = new SlidingWindow<Object, Long>(new LongSummer(), getWindowChunks());
    _latencies = new Latencies();
  }
  @Override
  public void emitCurrentWindow(BasicOutputCollector collector) {
    emitCurrentWindowCounts(collector);
  }

  @Override
  public void updateCurrentWindow(Tuple tuple) {
    countObj(tuple);
  }
  @Override public void prepare(Map stormConf, TopologyContext context) {
      _latencies = new Latencies();
      context.registerMetric("latencies", _latencies, 5);  
      jedis = new Jedis("130.104.230.108");
 }
  private void emitCurrentWindowCounts(BasicOutputCollector collector) {
    Map<Object, Long> counts = window.reduceThenAdvanceWindow();
    for (Entry<Object, Long> entry : counts.entrySet()) {
      Object obj = entry.getKey();
      Long count = entry.getValue();
      LOG.info(String.format("get %d %s in last %d seconds", count, obj, windowLengthInSeconds));
      collector.emit(new Values(obj, count));
    }
  }

  private void countObj(Tuple tuple) {
      Object obj = tuple.getValue(0);
    long creation = (Long) tuple.getValue(1);
    long time = System.currentTimeMillis();
    //LOG.info(String.format("Latency is %d",(int) (time-creation)));
    //System.out.println("Latency: " + (int)(time-creation));
    _latencies.add((int) (time-creation));
    //jedis.set((String)tuple.getValue(0), String.valueOf(time-creation));
    //jedis.rpush((String) obj, String.valueOf(time-creation));

    window.add(obj, (long) 1);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS_OBJ, FIELDS_CNT));
  }
}
