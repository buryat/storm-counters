package buryat.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import buryat.storm.bolt.CountBolt;
import buryat.storm.spout.RedisQueueSpout;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class RedisTopology {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("queue", new RedisQueueSpout(properties), 2);
        builder.setBolt("count", new CountBolt(), 2).shuffleGrouping("queue");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("redis", conf, builder.createTopology());
    }
}
