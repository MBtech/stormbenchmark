package storm.benchmark.lib.bolt;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.benchmark.util.Cons;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Basic terminal Bolt that just logs input fields.
 * 
 * @author Adrianos Dadis
 */
public class LoggingBolt extends BaseRichBolt {

   private static final long serialVersionUID = 1L;
   private static final Logger LOG = LoggerFactory.getLogger(LoggingBolt.class);

   private OutputCollector _collector;

   private boolean error = false;
   private String[] fields;

   @SuppressWarnings("rawtypes")
   @Override
   public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      _collector = collector;

      if (fields == null) {
         fields = new String[] {Cons.TUPLE_VAR_MSG};
      }
   }

   @Override
   public void execute(Tuple tuple) {
      if (error) {
         for (String field : fields) {
            LOG.error("{}: {}", field, tuple.getValueByField(field));
         }
      } else {
         for (String field : fields) {
            LOG.info("{}: {}", field, tuple.getValueByField(field));
         }
      }

      _collector.ack(tuple);
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {}

   public LoggingBolt withFields(String... fieldNames) {
      this.fields = fieldNames;
      return this;
   }

   public LoggingBolt withError(boolean errorCase) {
      this.error = errorCase;
      return this;
   }
}
