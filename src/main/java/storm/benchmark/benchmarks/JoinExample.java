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
import storm.benchmark.lib.bolt.JoinBolt;
import storm.benchmark.lib.spout.FileReadSpout;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;

import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.utils.Utils;
import storm.benchmark.lib.bolt.SplitBolt;
import storm.benchmark.lib.spout.FileSpout;

public class JoinExample extends StormBenchmark {

  private static final String WINDOW_LENGTH = "window.length";

  public static final String AD_SPOUT_ID = "Adspout";
  public static final String WEB_SPOUT_ID = "Webspout";

  public static final String SPOUT_NUM = "component.spout_num";
  public static final String JOIN_ID = "join";
  public static final String JOIN_NUM = "component.join_bolt_num";
  public static final String SPLIT_NUM = "component.split_bolt_num";
  public static final String AD_FILE = "spout.file.ad";
  public static final String WEB_FILE = "spout.file.web";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_JOIN_BOLT_NUM = 8;
  public static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = 1;

  private IRichSpout adspout;
  private IRichSpout webspout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int joinBoltNum = BenchmarkUtils.getInt(config, JOIN_NUM, DEFAULT_JOIN_BOLT_NUM);
    final int windowLength = BenchmarkUtils.getInt(config, WINDOW_LENGTH,DEFAULT_SLIDING_WINDOW_IN_SECONDS);
    final String adFile = Utils.getString(Utils.get(config, AD_FILE, "/ads"));
    final String webFile = Utils.getString(Utils.get(config, WEB_FILE, "/web"));
    final int spBoltNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_JOIN_BOLT_NUM);



    adspout = new FileSpout(BenchmarkUtils.ifAckEnabled(config), adFile);
    webspout = new FileSpout(BenchmarkUtils.ifAckEnabled(config), webFile);
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(AD_SPOUT_ID, adspout, spoutNum);
    builder.setSpout(WEB_SPOUT_ID, webspout, spoutNum);
    builder.setBolt("ad_split",new SplitBolt("user_id","ad_id"),spBoltNum).localOrShuffleGrouping(AD_SPOUT_ID);
    builder.setBolt("web_split",new SplitBolt("user_id","web_id"),spBoltNum).localOrShuffleGrouping(WEB_SPOUT_ID);
    builder.setBolt(JOIN_ID, new JoinBolt().withTumblingWindow(new Count(windowLength)), joinBoltNum).fieldsGrouping("ad_split", new Fields("user_id"))
           .fieldsGrouping("web_split", new Fields("user_id"));
    return builder.createTopology();
  }
}

