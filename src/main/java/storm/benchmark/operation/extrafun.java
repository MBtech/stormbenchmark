package storm.benchmark.operation;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class extrafun {

    public static float getavg(HashMap<Integer, Float> data) {
        float sum = 0;
        float avg = 0;
        for (Map.Entry<Integer, Float> entry : data.entrySet())
            sum += Double.valueOf(entry.getValue());
        avg = (float) sum / data.size();
        return avg;
    }

    //计算相似度
    public static float getweight(HashMap<Integer, Float> data,
                                   HashMap<Integer, Float> data_test)   //两个数据集，data 和data_set
    {
        float weight = 0;
        float avg1 = 0;
        float avg2 = 0;
        float measure1 = 0;
        float measure2 = 0;
        float measure1x2 = 0;
        float tmp1=0;
        float tmp2=0;
        List<Integer> id = new ArrayList<Integer>();   //ArrayList:动态数组
        for(Integer key:data_test.keySet()) {
            if(data.get(key)!=null) {
                avg1 += data.get(key);
                avg2 += data_test.get(key);
                id.add(key);   //向动态数组id中添加key
            }
        }
        if (id.size() != 0) {
            avg1 = avg1/id.size();  //data数据集中的key求均值
            avg2= avg2/id.size();   //data_set数据集中的key求均值
            for (Integer key : id) {
                tmp1 = data.get(key);   // tmp1:data数据集中的key
                measure1 += (tmp1 - avg1) * (tmp1 - avg1);
                tmp2 = data_test.get(key);    // tmp2:data_set数据集中的key
                measure2 += (tmp2 - avg2) * (tmp2 - avg2);
                measure1x2 += (tmp1 - avg1) * (tmp2 - avg2);
            }
            if (measure1 != 0 && measure2 != 0) {
                weight = (float) (measure1x2 / (Math.sqrt(measure1 * measure2)));  //计算用户user之间的相似度：sim(x,y)
            }
        }
        return weight;
    }


    public static String gethostname() {  //得到当前机器的hostname
        String host = "";
        InetAddress ia = null;
        try {
            ia = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        host = ia.getHostName();
        return host;
    }

    public static void sort(ArrayList<String> l, String str, int flag) {
        double value1 = Double.parseDouble(str.split(",")[1]);
        int i;
        if (flag == 0) {
            for (i = 0; i < l.size(); i ++) {
                double value2 = Double.parseDouble(l.get(i).split(",")[1]);
                if (value1 > value2) {
                    l.add(i + 1, str);
                    break;
                }
            }
        } else if (flag == 1) {
            for (i = l.size() - 1; i > -1; i --) {
                double value2 = Double.parseDouble(l.get(i).split(",")[1]);
                if (value1 < value2) {
                    l.add(i + 1, str);
                    break;
                }
            }
        }
    }
    //genmap(File input)
    public static HashMap genmap(File input) {
      // Map<userId, Map<movieId, rating>> => data
         HashMap<Integer, HashMap> data = new HashMap<Integer,HashMap>();   //data:Map类型的变量
        try {
            BufferedReader br = new BufferedReader(new FileReader(input));
            String str = br.readLine();
            while (str != null) {
                int userid = Integer.parseInt(str.split(",")[0]);  //userid
                final int itemid = Integer.parseInt(str.split(",")[1]);  // itemid
                final  float rate = Float.parseFloat(str.split(",")[2]);  //rate(打分)

                if (data.get(userid) == null) {   //get(Object key)):该方法返回指定键所映射的值。
                    data.put(userid, new HashMap<Integer, Float>(){{  //声明的一个Map类型的变量
                        put(itemid, rate);   //put(K key, V value) :key：是要保存到Map集合中的键名。value：是要保存到Map集合中对应键名的键值对象。
                    }});
                } else {
                    data.get(userid).put(itemid, rate);
                }
                str = br.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }
    public static ArrayList<String> hostslist(String[] list, int n) {
        ArrayList<String> y = new ArrayList<String>();
        Random r = new Random();
        int i = r.nextInt(18);   //nextInt(n)返回一个大于等于0小于n的随机数，即：0 <= nextInt(n) < n
        y.add(list[i]);
        int j  = 1;
        while (j < n) {
            i = r.nextInt(18);
            if (!y.contains(list[i])) {
                y.add(list[i]);
                j ++;
            }
        }
        return y;
    }

}

