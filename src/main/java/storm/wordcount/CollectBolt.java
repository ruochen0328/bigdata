package storm.wordcount;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yangwensheng
 * @date 2018/7/1 20:06
 */

public class CollectBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private HashMap<String,Integer> map=new HashMap<String,Integer>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer num = tuple.getInteger(1);
        if (map.containsKey(word)){
            Integer oldnum = map.get(word);
            map.put(word, oldnum+num);
        }else{
            map.put(word,num);
        }
        System.out.println(word+"---------->"+map.get(word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
