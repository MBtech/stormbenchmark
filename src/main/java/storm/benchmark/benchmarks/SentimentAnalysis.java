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
import storm.benchmark.lib.bolt.RankBolt;
import storm.benchmark.lib.bolt.MergeObjectsBolt;
import storm.benchmark.lib.bolt.RollingCountObjects;
import storm.benchmark.lib.bolt.PositiveBolt;
import storm.benchmark.lib.bolt.NegativeBolt;
import storm.benchmark.lib.bolt.ScoreBolt;
import storm.benchmark.lib.bolt.LoggingBolt;
import storm.benchmark.lib.bolt.StemmingBolt;
import storm.benchmark.util.Cons;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.utils.Utils;

public class SentimentAnalysis extends StormBenchmark {

  private static final String WINDOW_LENGTH = "window.length";
  private static final String EMIT_FREQ = "emit.frequency";

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String STEM_ID = "stemming";
  public static final String STEM_NUM = "component.stemming_bolt_num";
  public static final String POSITIVE_ID = "positive";
  public static final String POSITIVE_NUM = "component.positive_bolt_num";
  public static final String NEGATIVE_ID = "negative";
  public static final String NEGATIVE_NUM = "component.negative_bolt_num";
  public static final String SCORE_ID = "score";
  public static final String SCORE_NUM = "component.score_bolt_num";
  public static final String FILE_CONFIG = "spout.file";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_SP_BOLT_NUM = 8;
  public static final int DEFAULT_RC_BOLT_NUM = 8;

  private IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int stemBoltNum = BenchmarkUtils.getInt(config, STEM_NUM, DEFAULT_SP_BOLT_NUM);
    final int posBoltNum = BenchmarkUtils.getInt(config, POSITIVE_NUM, DEFAULT_RC_BOLT_NUM);
    final int negBoltNum = BenchmarkUtils.getInt(config, NEGATIVE_NUM, DEFAULT_RC_BOLT_NUM);
    final int scoreBoltNum = BenchmarkUtils.getInt(config, SCORE_NUM, DEFAULT_RC_BOLT_NUM);
    final int logBoltNum = BenchmarkUtils.getInt(config, "component.logging_bolt_num", DEFAULT_RC_BOLT_NUM);
    final int windowLength = BenchmarkUtils.getInt(config, WINDOW_LENGTH,
            RollingBolt.DEFAULT_SLIDING_WINDOW_IN_SECONDS);
    final int emitFreq = BenchmarkUtils.getInt(config, EMIT_FREQ,
            RollingBolt.DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    final String filename = Utils.getString(Utils.get(config, FILE_CONFIG, "/A_Tale_of_Two_City.txt"));

    spout = new FileReadSpout(BenchmarkUtils.ifAckEnabled(config), filename);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
//    builder.setBolt("file sink", new FileSink(), 3).localOrShuffleGrouping(COUNTER_ID);   
        builder.setBolt(STEM_ID, new StemmingBolt(), stemBoltNum)
              .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(POSITIVE_ID, new PositiveBolt(), posBoltNum).localOrShuffleGrouping(STEM_ID);   //RollingCountObjects
        builder.setBolt(NEGATIVE_ID, new NegativeBolt(), negBoltNum).localOrShuffleGrouping(POSITIVE_ID);
        builder.setBolt(SCORE_ID, new ScoreBolt(), scoreBoltNum).localOrShuffleGrouping(NEGATIVE_ID);
        builder.setBolt("logging", new LoggingBolt().withFields(Cons.TUPLE_VAR_MSG, Cons.TUPLE_VAR_SCORE), logBoltNum)
            .localOrShuffleGrouping(SCORE_ID);
	return builder.createTopology();
  }
}
