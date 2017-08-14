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

package storm.benchmark.benchmarks;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.benchmark.benchmarks.common.WordCount;
import storm.benchmark.lib.bolt.RollingCountBolt;
import storm.benchmark.lib.bolt.RollingBolt;
import storm.benchmark.lib.bolt.FileSink;
import storm.benchmark.lib.spout.FileReadSpout;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.benchmarks.Nop;

import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.utils.Utils;

public class RollingCountShuffle extends StormBenchmark {

  private static final String WINDOW_LENGTH = "window.length";
  private static final String EMIT_FREQ = "emit.frequency";

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String SPLIT_ID = "split";
  public static final String SPLIT_NUM = "component.split_bolt_num";
  public static final String COUNTER_ID = "rolling_count";
  public static final String COUNTER_NUM = "component.rolling_count_bolt_num";
  public static final String FILE_CONFIG = "spout.file";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_SP_BOLT_NUM = 8;
  public static final int DEFAULT_RC_BOLT_NUM = 8;

  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int spBoltNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_SP_BOLT_NUM);
    final int rcBoltNum = BenchmarkUtils.getInt(config, COUNTER_NUM, DEFAULT_RC_BOLT_NUM);
    final int windowLength = BenchmarkUtils.getInt(config, WINDOW_LENGTH,
            RollingBolt.DEFAULT_SLIDING_WINDOW_IN_SECONDS);
    final int emitFreq = BenchmarkUtils.getInt(config, EMIT_FREQ,
            RollingBolt.DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    final String filename = Utils.getString(Utils.get(config, FILE_CONFIG, "/A_Tale_of_Two_City.txt"));

    spout = new FileReadSpout(BenchmarkUtils.ifAckEnabled(config), filename);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(SPLIT_ID, new WordCount.SplitSentence(), spBoltNum)
            .localOrShuffleGrouping(SPOUT_ID);
    builder.setBolt(COUNTER_ID, new WordCount.Count(), rcBoltNum).shuffleGrouping(SPLIT_ID);
    builder.setBolt("aggregator", new WordCount.ACount(), 1).fieldsGrouping(COUNTER_ID, new Fields(WordCount.SplitSentence.FIELDS));
//    builder.setBolt(COUNTER_ID, new RollingCountBolt(windowLength, emitFreq), rcBoltNum)
//            .fieldsGrouping(SPLIT_ID, new Fields(WordCount.SplitSentence.FIELDS));
//    builder.setBolt("file sink", new FileSink(), 3).localOrShuffleGrouping(COUNTER_ID);   
	return builder.createTopology();
  }
}
