/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm.benchmark.lib.bolt;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

/**
 *
 * @author mb
 */
public class JoinBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tw) {
        HashMap<String, String> ads = new HashMap<String, String>();
        HashMap<String, String> web = new HashMap<String, String>();

        for (Tuple tuple : tw.get()) {
            if (tuple.getSourceComponent().equals("ad_split")) {
                ads.put((String) tuple.getValues().get(0), (String) tuple.getValues().get(1));
            }
            if (tuple.getSourceComponent().equals("web_split")) {
                web.put((String) tuple.getValues().get(0), (String) tuple.getValues().get(1));
            }
            //System.out.println(tuple.toString());
        }
        for (String userID : ads.keySet()) {
            if (web.get(userID) != null) {
                collector.emit(new Values(ads.get(userID), web.get(userID)));
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("adID", "webID"));
    }

}

