package storm.benchmark.benchmarks;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.lib.bolt.FinalCfBolt;
import storm.benchmark.lib.bolt.FinalMergeBolt;
import storm.benchmark.lib.bolt.GroupMergeBolt;
import storm.benchmark.lib.spout.CfSpout;

import java.io.File;
import java.io.FileInputStream;
import org.apache.storm.metric.LoggingMetricsConsumer;

public class CfByUser extends StormBenchmark {

  private static final String WINDOW_LENGTH = "window.length";
  private static final String EMIT_FREQ = "emit.frequency";

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String SPLIT_ID = "split";
  public static final String SPLIT_NUM = "component.split_bolt_num";
  public static final String COUNTER_ID = "rolling_count";
  public static final String COUNTER_NUM = "component.rolling_count_bolt_num";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_SP_BOLT_NUM = 8;
  public static final int DEFAULT_RC_BOLT_NUM = 8;


  @Override
  public StormTopology getTopology(Config config) {

        String plan = String.valueOf(BenchmarkUtils.getInt(config,"cfbyuser.plan",0));
        int freq = BenchmarkUtils.getInt(config,"cfbyuser.freq",10);  

        float per = BenchmarkUtils.getInt(config,"cfbyuser.boltratio",1);  
        int boltnum = BenchmarkUtils.getInt(config,"cfbyuser.group.boltnum",1);
        int pro_num = (int) (boltnum * per); 

        String testfile = "/home/ubuntu/bilal/storm-benchmark/test_rating.csv";
        String trainingfile = "/home/ubuntu/bilal/storm-benchmark/data_rating.csv";
        int spoutThreads = BenchmarkUtils.getInt(config,"cfbyuser.cfspout.threads",1);
        int FinalCfboltThreads = BenchmarkUtils.getInt(config,"cfbyuser.FinalCfbolt.threads",1);
        int GroupMergeboltThreads = BenchmarkUtils.getInt(config,"cfbyuser.GroupMergebolt.threads",1);
        int FinalMergeboltThreads = BenchmarkUtils.getInt(config,"cfbyuser.FinalMergebolt.threads",1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("cf-spout1", new CfSpout(new File(testfile), 1, pro_num, freq), spoutThreads);

        builder.setBolt("final-cf-bolt1", new FinalCfBolt(new File(trainingfile), plan), FinalCfboltThreads)
                .allGrouping("cf-spout1");
        builder.setBolt("group-merge-bolt1", new GroupMergeBolt(pro_num), GroupMergeboltThreads).shuffleGrouping("final-cf-bolt1");
        builder.setBolt("final-merge-bolt", new FinalMergeBolt(), FinalMergeboltThreads)
                .shuffleGrouping("group-merge-bolt1");
        return builder.createTopology();
}
}
