package storm.benchmark.lib.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/** 

 */

public class RankBolt implements IBasicBolt {

	/**
	 * @Fields serialVersionUID : TODO
	 */
	private static final long serialVersionUID = -4448088854486947656L;

	@SuppressWarnings("rawtypes")
	List<List> _rankings = new ArrayList<List>();
	
	int _count;
	Long _lastTime = null;
	
	public RankBolt(int n) {
		// TODO Auto-generated constructor stu
		_count = n;
	}
	
	@SuppressWarnings({"rawtypes" })
	private int compare1(List one, List two) {
		long valueOne = (Long) one.get(1);
		long valueTwo = (Long) two.get(1);
		
		long delta = valueTwo - valueOne;
		if(delta > 0) {
			return 1;
		} else if (delta < 0) {
			return -1;
		} else {
			return 0;
		}
	}
	
	private Integer find(Object tag) {
		for(int i = 0; i < _rankings.size(); ++i) {
			Object cur = _rankings.get(i).get(0);
			if(cur.equals(tag)) {
				return i;
			}
		}
		return null;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("list"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {  //Tuple:"obj", "count"
		// TODO Auto-generated method stub
		Object tag = input.getValue(0);   // 获取obj
		Integer existingIndex = find(tag);  //查找obj
		
		if(null != existingIndex) {
			_rankings.set(existingIndex, input.getValues());
		} else {
			_rankings.add(input.getValues());
		}
		
		Collections.sort(_rankings, new Comparator<List>() {
			@Override
			public int compare(List o1, List o2) {
				// TODO Auto-generated method stub
				return compare1(o1, o2);
			}
		});
		
		if(_rankings.size() > _count) {
			_rankings.remove(_count);
		}
		
		long currentTime = System.currentTimeMillis();
		if(_lastTime == null || currentTime >= _lastTime + 2000) {
			collector.emit(new Values(JSONValue.toJSONString(_rankings))); //JSON字符串
			System.out.println(JSONValue.toJSONString(_rankings));
			_lastTime = currentTime;
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	
}



