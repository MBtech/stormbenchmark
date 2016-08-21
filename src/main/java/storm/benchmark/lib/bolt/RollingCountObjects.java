package storm.benchmark.lib.bolt;

/**
 * Created by ACS on 2015/11/26.
 */
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 周期计算统计
 */
public class RollingCountObjects  extends BaseRichBolt {

    private HashMap<Object, long[]> _objectCounts = new HashMap<Object, long[]>();
    private int _numBuckets;
    private transient Thread cleaner;
    private OutputCollector _collector;
    private int _trackMinutes;

    public RollingCountObjects(int numBuckets, int trackMinutes) {
        _numBuckets = numBuckets;
        _trackMinutes = trackMinutes;
    }

    public long totalObjects (Object obj) {
        long[] curr = _objectCounts.get(obj);
        long total = 0;
        for (long l: curr) {
            total+=l;
        }
        return total;
    }

    public int currentBucket (int buckets) {
        return (currentSecond()  / secondsPerBucket(buckets)) % buckets;
    }

    public int currentSecond() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public int secondsPerBucket(int buckets) {
        return (_trackMinutes * 60 / buckets);
    }

    public long millisPerBucket(int buckets) {
        return (long) secondsPerBucket(buckets) * 1000;
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        cleaner = new Thread(new Runnable() {
            @SuppressWarnings("unchecked")
            public void run() {
                Integer lastBucket = currentBucket(_numBuckets);


                while(true) {
                    int currBucket = currentBucket(_numBuckets);
                    if(currBucket!=lastBucket) {
                        int bucketToWipe = (currBucket + 1) % _numBuckets;
                        synchronized(_objectCounts) {
                            Set objs = new HashSet(_objectCounts.keySet());
                            for (Object obj: objs) {
                                long[] counts = _objectCounts.get(obj);
                                long currBucketVal = counts[bucketToWipe];
                                counts[bucketToWipe] = 0; //  *这行代码很关键*
                                long total = totalObjects(obj);
                                if(currBucketVal!=0) {
                                    _collector.emit(new Values(obj, total));
                                }
                                if(total==0) {
                                    _objectCounts.remove(obj);
                                }
                            }
                        }
                        lastBucket = currBucket;
                    }
                    long delta = millisPerBucket(_numBuckets) - (System.currentTimeMillis() % millisPerBucket(_numBuckets));
                    Utils.sleep(delta);
                }
            }
        });
        cleaner.start();
    }

    public void execute(Tuple tuple) {  // tuple:words

        Object obj = tuple.getValue(0);
        int bucket = currentBucket(_numBuckets);
        synchronized(_objectCounts) {
            long[] curr = _objectCounts.get(obj);
            if(curr==null) {
                curr = new long[_numBuckets];
                _objectCounts.put(obj, curr);
            }
            curr[bucket]++;
            _collector.emit(new Values(obj, totalObjects(obj), tuple.getValue(1)));
            _collector.ack(tuple);
        }
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count", "timestamp"));
    }

}

