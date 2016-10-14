package storm.benchmark.lib.bolt;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import storm.benchmark.util.Cons;

/**
 * Simple Bolt that check words of incoming sentence and mark sentence with a positive score.
 * 
 * @author Adrianos Dadis
 * 
 */
public class PositiveBolt extends BaseBasicBolt {
   private static final long serialVersionUID = 1L;
   private static final Logger LOG = LoggerFactory.getLogger(PositiveBolt.class);

   private transient ObjectMapper mapper;

   @SuppressWarnings("rawtypes")
   @Override
   public void prepare(Map stormConf, TopologyContext context) {
      mapper = new ObjectMapper();
      mapper.setSerializationInclusion(Include.NON_NULL);
   }

   @Override
   public void execute(Tuple tuple, BasicOutputCollector collector) {

      try {
         ObjectNode node = (ObjectNode) mapper.readTree(tuple.getString(0));

         String[] words = node.path(Cons.MOD_TXT).asText().split(" ");
         int wordsSize = words.length;
         int positiveWordsSize = 0;
         for (String word : words) {
            if (PositiveWords.get().contains(word)) {
               positiveWordsSize++;
            }
         }

         node.put(Cons.NUM_POSITIVE, (double) positiveWordsSize / wordsSize);

         collector.emit(new Values(node.toString()));

      } catch (Exception e) {
         LOG.error("Cannot process input. Ignore it", e);
      }

   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(Cons.TUPLE_VAR_MSG));
   }

   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }

   private static class PositiveWords {
      private Set<String> positiveWords;
      private static PositiveWords _singleton;

      private PositiveWords() {
         positiveWords = new HashSet<String>();

         //Add more "positive" words and load from file or database
         positiveWords.add("admire");
         positiveWords.add("bonus");
         positiveWords.add("calm");
         positiveWords.add("good");
         positiveWords.add("accept");
         positiveWords.add("happiness");
         positiveWords.add("amazing");
      }

      static PositiveWords get() {
         if (_singleton == null) {
            synchronized (PositiveWords.class) {
               if (_singleton == null) {
                  _singleton = new PositiveWords();
               }
            }
         }

         return _singleton;
      }

      boolean contains(String key) {
         return get().positiveWords.contains(key);
      }
   }

}
