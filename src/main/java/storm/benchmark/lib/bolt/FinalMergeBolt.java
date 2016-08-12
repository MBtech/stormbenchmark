package storm.benchmark.lib.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 7/14/15.
 * modified by rain on 15-11-23
 */
public class FinalMergeBolt extends BaseRichBolt {
    OutputCollector collector;
    HashMap<String, Float[]> data;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        data = new HashMap<String,Float[]>();
    }

    @Override
    public void execute(Tuple tuple) {
        String testinfo = tuple.getString(0);  //testinfo
        float avg = tuple.getFloat(1);
        float tmp1 = tuple.getFloat(2);
        float tmp2 = tuple.getFloat(3);
        long start = tuple.getLong(4);
        Float[] tmp;
        if(data.get(testinfo) == null) {
            tmp = new Float[]{1.0f, tmp1, tmp2};
            data.put(testinfo, tmp);
        } else {
            tmp = data.get(testinfo);
            tmp[0] += 1;
            tmp[1] += tmp1;
            tmp[2] += tmp2;
            if (tmp[0] == 6) {
                float pre;
                if (tmp[2] != 0)
                    pre = avg + tmp[1] / tmp[2];
                else
                    pre = avg;
                if (pre > 5)
                    pre = 5;
                if (pre < 1)
                    pre = 1;
                long totaltime = System.currentTimeMillis() - start;
                System.out.println("totaltime:" + testinfo + "," + pre + ";" + totaltime);
                data.remove(testinfo);
            } else {
                data.put(testinfo, tmp);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

