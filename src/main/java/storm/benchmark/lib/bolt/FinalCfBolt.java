package storm.benchmark.lib.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.benchmark.operation.extrafun;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wjw on 15-6-8.
 * modified by rain on 15-11-23
 */
public class FinalCfBolt extends BaseRichBolt {
    ArrayList<HashMap<Integer, HashMap>> trainset = new ArrayList<HashMap<Integer, HashMap>>(); //18个完整训练集读入，每一个训练集存到一个HashMap，trainset存放所有的HashMap
    HashMap<Integer, HashMap> part_user_map = new HashMap<Integer, HashMap>();
    HashMap<Integer, HashMap<Integer, Float>> test_user_info_map = new HashMap<Integer, HashMap<Integer, Float>>();  //存放测试user的信息
    HashMap<String, String[]> codeindex = new HashMap<String, String[]>();
    OutputCollector collector;  //存放传给merge bolt的信息
   // File dir;  //训练集文件
    File testuserinfo;  //测试集文件
  //  JedisPool pool; //Redis的jave客户端接口连接池
   // String host;
    String plan;
    public FinalCfBolt( File arg2, String plan) {
     //   this.dir = arg1;  //训练集文件夹
        this.testuserinfo = arg2;  //测试集文件
        this.plan = plan;
    }
    @Override
    //初始化各种变量
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        test_user_info_map = extrafun.genmap(testuserinfo);  //arg2
       // host = extrafun.gethostname();

        //处理完整训练文件
        part_user_map = extrafun.genmap(new File("/home/cf-data/bigtrain.txt"));

    }

    @Override
    public void execute(Tuple tuple) {
        String testinfo = tuple.getString(0); // testinfo:来源于cfspout发出的tuple：user_test,start_time
        long start = tuple.getLong(1);
      //  ArrayList<String> hosts = (ArrayList<String>) tuple.getValue(2);
        //String testinfo = tuple.getString(0); // [user,item,rate]
        int test_user =  Integer.parseInt(testinfo.split(",")[0]);   //test_user：用户id
      //  System.out.println(test_user);
        int test_item = Integer.parseInt(testinfo.split(",")[1]);   //test_item:物品id

        HashMap<Integer, Float> test_user_info = test_user_info_map.get(test_user);  // test_user_info：来源于输入参数arg2(测试文件夹)
        float test_user_avg = extrafun.getavg(test_user_info);   //对测试文件arg2中的用户信息 test_user_info求均值
        float tmp1 = 0;
        float tmp2 = 0;

        //key here is a userId
        for (Integer key : part_user_map.keySet()) {
            HashMap<Integer, Float> user_info = part_user_map.get(key);   //user_info：来源于bigtrain.txt（训练集）
            Float rate = user_info.get(test_item); //返回test_item对应的打分值
            if (rate != null) {
                float weight = extrafun.getweight(user_info, test_user_info);  //weight:用户之间的相似度
                float user_avg = extrafun.getavg(user_info);   //对训练集中的user_info求均值
                tmp1 += (rate - user_avg) * weight;  //
                tmp2 += Math.abs(weight);  //
            }
         }
        long bolttime = System.currentTimeMillis() - start;
       // System.out.println(bolttime);

      //   System.out.println("bolttime:" + bolttime+","+testinfo + "," + test_user_avg + "," + tmp1+ ","+ tmp2 + ",starttime:"+start);

        collector.emit(new Values(testinfo, test_user_avg, tmp1, tmp2, bolttime, start));


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("testinfo", "avg", "tmp1", "tmp2", "bolttime", "begintime"));
    }
}

