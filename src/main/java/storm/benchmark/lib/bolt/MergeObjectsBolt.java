package storm.benchmark.lib.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**

 */

public class MergeObjectsBolt implements IBasicBolt {

	public static Logger LOG = Logger.getLogger(MergeObjectsBolt.class);
	/**
	 * @Fields serialVersionUID : TODO
	 */
	private static final long serialVersionUID = -1729847516839455550L;

	//JedisPool pool;
	//Jedis jedis;

	@SuppressWarnings("rawtypes")
	private List<List> _rankings = new ArrayList<List>();
	int _count = 10;
	Long _lastTime;

	public  MergeObjectsBolt(int n) {
		// TODO Auto-generated constructor stub
		_count = n;
	}

	@SuppressWarnings("rawtypes")
	private int compare1(List one, List two) {
		long valueOne = (Long) one.get(1);
		long valueTwo = (Long) two.get(1);

		long dalta = valueTwo - valueOne;

		if(dalta > 0) {
			return 1;
		} else if (dalta < 0) {
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
		declarer.declare(new Fields("merge"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
	//	pool = new JedisPool("192.168.200.216", 6379);
	//	jedis = pool.getResource();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {   //Tuple；ranking
		// TODO Auto-generated method stub
		List<List> merging = (List) JSONValue.parse(input.getString(0));
		for(List pair : merging) {
			Integer existingIndex = find(pair.get(0));
			if(null != existingIndex) {
				_rankings.set(existingIndex, pair);
			} else {
				_rankings.add(pair);
			}
			Collections.sort(_rankings, new Comparator<List>() {
				@Override
				public int compare(List o1, List o2) {
					// TODO Auto-generated method stub
					return compare1(o1, o2);
				}
			});

			if(_rankings.size() > _count) {
				_rankings.subList(_count, _rankings.size()).clear();
			}
		}

		long currentTime = System.currentTimeMillis();
		if(_lastTime == null || currentTime >= _lastTime + 2000) {
			String fullRankings = JSONValue.toJSONString(_rankings);
			collector.emit(new Values(fullRankings));
			LOG.info("Rankings:\t"+fullRankings);
			System.out.println("Rankings:\t"+fullRankings);
			_lastTime = currentTime;
			//jedis.lpush("result", fullRankings);
		}

		/**
		 * 保持固定长度，避免内存占用过多溢�?
		 */
		/*
		long len = jedis.llen("result");
		if(len > 100) {
			jedis.rpop("result");
		}*/
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}


}

