package storm.benchmark.lib.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.log4j.Logger;
import storm.benchmark.tools.FileReader;


import java.util.Map;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import storm.benchmark.metrics.Latencies;
import storm.benchmark.tools.HDFSFileReader;

public class FileSpout extends BaseRichSpout {

    private static final Logger LOG = Logger.getLogger(FileReadSpout.class);
    private static final long serialVersionUID = -2582705611472467172L;

    //public static final String DEFAULT_FILE = "/resources/A_Tale_of_Two_City.txt";
    public static final String DEFAULT_FILE = "hdfs://nimbus1:9000/tweet_data.txt";

    public static final boolean DEFAULT_ACK = false;
    public String FIELD1 = "sentence";
    public String FIELD2 = "sentence";

    public final boolean ackEnabled;
    public HDFSFileReader reader;
    private SpoutOutputCollector collector;
    public static Configuration conf = new Configuration();
    private long count = 0;

    transient Latencies _latencies;
    public static final HashMap<Long, Long> timeStamps
            = new HashMap<Long, Long>();

    public FileSpout() {
        this(DEFAULT_ACK, DEFAULT_FILE);
    }

    public FileSpout(boolean ackEnabled) {
        this(ackEnabled, DEFAULT_FILE);
    }
    public FileSpout(boolean ackEnabled, String file) {
        this.ackEnabled = ackEnabled;
        this.reader =
                new HDFSFileReader("hdfs://nimbus1:9000" + file);
    }
    
    public FileSpout(boolean ackEnabled, String file, String field1, String field2) {
        this.ackEnabled = ackEnabled;
        this.reader =
                new HDFSFileReader("hdfs://nimbus1:9000" + file);
        this.FIELD1 = field1;
        this.FIELD2 = field2;
    }

    public FileSpout(boolean ackEnabled, HDFSFileReader reader) {
        this.ackEnabled = ackEnabled;
        this.reader = reader;
    }


    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        //_latencies = new Latencies();
        //context.registerMetric("latencies", _latencies, 10);
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (ackEnabled) {
            collector.emit(new Values(reader.nextLine(),System.currentTimeMillis()), count);
            count++;
        } else {
            collector.emit(new Values(reader.nextLine()));
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line","timestamp"));
    }

}
