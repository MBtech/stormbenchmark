package storm.benchmark.lib.bolt;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileNotFoundException;

import java.util.Map;
import java.util.HashMap;

import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.google.gson.Gson;
public class FileSink extends BaseRichBolt {

    public static Logger LOG = LoggerFactory.getLogger(FileSink.class);
    OutputCollector _collector;
    static transient FileWriter _filewriter;
    boolean _ack;
    //Path file = Paths.get("/home/ubuntu/bilal/results.log");
    static transient File fileobject;         
    
   static transient MappedByteBuffer buffer;
        //Write the content using put methods
    public FileSink(String file, boolean ack) {
        _ack = ack;
        fileobject =  new File("/home/ubuntu/bilal/results.log");

//        try {
//            _filewriter = new FileWriter("/home/ubuntu/bilal/results.log");
//            LOG.debug("File opened/created successfully.");
//        } catch (IOException e) {
//            
//            LOG.error("Unable to open file location.");
//        }
    }

    public FileSink(){
        try {
            _ack = true;
            fileobject =  new File("/home/ubuntu/bilal/results.log");
            fileobject.delete();
            FileChannel fileChannel = new RandomAccessFile(fileobject, "rw").getChannel();
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096 * 8 * 8);
        } catch (FileNotFoundException ex) {
            java.util.logging.Logger.getLogger(FileSink.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(FileSink.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (_ack) {
            _collector.ack(tuple);
        }
        Map<String, Object> map = new HashMap<String, Object>();
        for (String field : tuple.getFields()) {
            map.put(field, tuple.getValueByField(field));
        }
        buffer.put(map.toString().getBytes());
        
    }

    @Override
    public void cleanup() {
        // Close file writers.
        try {
            _filewriter.close();
            LOG.debug("File closed successfully.");
        } catch (IOException e) {
            LOG.error("Unable to close file.");
        }
    }

    // Declare the output schema.
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Nothing.
    }
}

