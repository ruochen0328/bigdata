package storm.wordcount;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author yangwensheng
 * @date 2018/7/1 19:51
 */

public class WordCountTopologMain {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),2);
        topologyBuilder.setBolt("splitBolt",new SplitBolt(),3).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("collectBolt",new CollectBolt(),4)
                .fieldsGrouping("splitBolt",new Fields("word"));

        Config config=new Config();
        config.setNumWorkers(2);
        String topologyName="wordcount";
        if (args.length>0){
            topologyName=args[0];
        }
        StormSubmitter.submitTopology(topologyName,config,topologyBuilder.createTopology());
        //本地模式
//        LocalCluster localCluster=new LocalCluster();
////        localCluster.submitTopology(topologyName,config,topologyBuilder.createTopology());
    }
}
