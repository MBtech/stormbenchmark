/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm.benchmark.lib.bolt;

import java.util.Arrays;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author mb
 */
public class SplitBolt extends BaseBasicBolt{
    public String FIELD1 = "sentence";
    public String FIELD2 = "sentence";
    
    public SplitBolt(String field1, String field2){
        this.FIELD1 = field1;
        this.FIELD2 = field2;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELD1, FIELD2));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        String[] words = tuple.getString(0).split(",");
        boc.emit(new Values(words[0],words[1]));
    }


    
}

