package buryat.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import buryat.storm.bolt.MultiSlotCountBolt;
import buryat.storm.spout.AWSRedisQueuesSpout;
import buryat.storm.spout.RedisQueueSpout;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public final class MultiSlotQueueCountersTopology {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("queue", new RedisQueueSpout(properties), 1);
        builder.setBolt("count", new MultiSlotCountBolt(properties), 1).shuffleGrouping("queue");

        Config conf = new Config();

        if (args.length == 1) {
            conf.setNumWorkers(4);
            conf.setMaxSpoutPending(5000);

            StormSubmitter.submitTopology("multiSlotQueueCounters", conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("multiSlotQueueCounters", conf, builder.createTopology());

            /*Utils.sleep(10000);
            cluster.killTopology("multiSlotQueueCounters");
            cluster.shutdown();*/
        }
    }
}
