package storm.benchmark.lib;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CfSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	ArrayList<String> data = new ArrayList<String>();
	int i = 0;
	int freq;
	File testfile;
	int flag;
	int index;
	int pro_num;
	public CfSpout(File testfile, int i, int num, int f) {
		this.testfile = testfile;   //测试文件
		this.index = i;
		this.pro_num = num;   //每个分组的处理个数
		this.freq = f;  //发射频率
	}
	@Override
	/**
	 * open方法会在SpoutTracker类中被调用每调用一次就可以向storm集群中发射一条数据（一个tuple元组）
	 * 该方法会被不停的调用
	 */
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		BufferedReader reader;
		String str;
		try {
			reader = new BufferedReader(new FileReader(testfile));
			try {
				while ((str = reader.readLine()) != null)
					data.add(str);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		if(flag > 60) {    //空跑的时间，60秒
			//ArrayList<String> hosts = extrafun.hostslist(Machines.HOSTNAMES[index], pro_num);  //hosts
			//String hosts="172.17.0.2";
			long start_time = System.currentTimeMillis();  //开始时间
			int j;
			if (i < 4000) {  //4000,总共发射个数的上限
				j = i % data.size();
				// 调用发射方法emit()
				//_collector.emit(new Values(data.get(j), start_time, hosts));
				_collector.emit(new Values(data.get(j), start_time));

				i++;
			}
			Utils.sleep((long) (1000 / freq));   //Freq代表1秒发射的个数
		} else {  //空跑阶段，1秒发一个
			flag ++;
			Utils.sleep(1000);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	 //	declarer.declare(new Fields("user_test", "begintime", "hosts"));
		declarer.declare(new Fields("user_test", "begintime"));
	}

}
