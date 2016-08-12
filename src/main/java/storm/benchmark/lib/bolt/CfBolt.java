package storm.benchmark.lib.bolt;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.benchmark.operation.extrafun;

//cfBolt: 处理压缩文件
public class CfBolt extends BaseRichBolt {
    OutputCollector collector;
    HashMap<Integer, HashMap<Integer, Float>> part_user_map = new HashMap<Integer, HashMap<Integer, Float>>();
    HashMap<Integer, HashMap<Integer, Float>> test_user_info_map = new HashMap<Integer, HashMap<Integer, Float>>();
    File trainfile;  //训练文件
    File testuserinfo;    //测试用户信息
   String trainid;  //处理压缩训练集文件的编号
    String host;
    public CfBolt(File trainfile, File testuserinfo) {
        this.trainfile = trainfile;  //训练文件
        this.testuserinfo = testuserinfo; //测试用户信息
    }

    @Override
    public void execute(Tuple tuple) {
		long boltstart = System.currentTimeMillis();
        long start = tuple.getLong(1);
        String testinfo = tuple.getString(0); // (int)[user,item,rate]
        int test_user =  Integer.parseInt(testinfo.split(",")[0]);  //根据‘，’分割出user
        int test_item = Integer.parseInt(testinfo.split(",")[1]); // [item]
        HashMap<Integer, Float> test_user_info = test_user_info_map.get(test_user);
        ArrayList<String> w = new ArrayList<String>();
        for (Integer key : part_user_map.keySet()) {  //测试所有的训练集的users，找到共同打分的users
            HashMap<Integer, Float> user_info = part_user_map.get(key);
            Float rate = user_info.get(test_item);
            if (rate != null) {
                float weight = extrafun.getweight(user_info, test_user_info);
                float user_avg = extrafun.getavg(user_info);
                w.add(key + "," + weight + "," + rate + "," + user_avg);
                //w：训练集里面有共同打分的users，每个user记录了Id，权重，打分，平均打分
            }
        }
        long bolttime = System.currentTimeMillis() - boltstart;
       collector.emit(new Values(testinfo, host, bolttime, w, trainid, start));
     //   collector.emit(new Values(testinfo, host, bolttime, w, start));
    }

    @Override
    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.host = extrafun.gethostname();  //host
        part_user_map = extrafun.genmap(trainfile);  //读取trainfile
        test_user_info_map = extrafun.genmap(testuserinfo);

       try {
            BufferedReader br = new BufferedReader(new FileReader("/home/cf-data/trainid")); //trainid，文件内容是train.txt对应的分割文件的编号，1，2，…18
            trainid = br.readLine();
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("testinfo", "hostname", "time", "weight", "trainid", "begintime"));
       // declarer.declare(new Fields("testinfo", "hostname", "time", "weight", "begintime"));
    }

}
