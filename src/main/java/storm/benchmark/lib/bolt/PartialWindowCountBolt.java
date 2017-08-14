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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.lang.NullPointerException;
import storm.benchmark.metrics.Latencies;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.commons.lang3.tuple.Pair;
/**
 * forked from RollingCountBolt in storm-starter
 */

public class PartialWindowCountBolt extends BaseWindowedBolt {

  private static final long serialVersionUID = -903093673694769540L;
  private static final Logger LOG = Logger.getLogger(PartialWindowCountBolt.class);
  public static final String FIELDS_OBJ = "obj";
  public static final String FIELDS_CNT = "count";
  public static final String TIMESTAMP = "timestamp";
 
  private OutputCollector collector;
 
  @Override public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
 }
  

  @Override
  public void execute(TupleWindow inputWindow){
     HashMap<Object, Pair<Long,Long>> countMap = new HashMap<Object, Pair<Long, Long>>();
     for(Tuple tuple : inputWindow.get()){
     	Object obj = tuple.getValue(0);
        if (countMap.get(obj) !=null){
            countMap.put(obj, Pair.of(countMap.get(obj).getKey() + 1, (Long)tuple.getValue(1)) );  
	    
        }
	else{
	    countMap.put(obj, Pair.of(1l,(Long)tuple.getValue(1)) );
	}
     }
    
    for (Entry<Object, Pair<Long, Long>> entry : countMap.entrySet()) {
      Object obj = entry.getKey();
      Long count = entry.getValue().getKey();
      Long time = entry.getValue().getValue();
//      LOG.info(String.format("get %d %s in last %d seconds", count, obj, windowLengthInSeconds));

      collector.emit(new Values(obj, count, time));
    }  

  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS_OBJ, FIELDS_CNT, TIMESTAMP));
  }
}
