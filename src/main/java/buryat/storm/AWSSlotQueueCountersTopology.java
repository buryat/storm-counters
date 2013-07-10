package buryat.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import buryat.storm.bolt.SlotCountBolt;
import buryat.storm.spout.AWSRedisQueuesSpout;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public final class AWSSlotQueueCountersTopology {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("queues", new AWSRedisQueuesSpout(properties), 4);
        builder.setBolt("count", new SlotCountBolt(properties), 4).shuffleGrouping("queues");

        Config conf = new Config();

        if (args.length == 1) {
            conf.setNumWorkers(4);
            conf.setMaxSpoutPending(5000);

            StormSubmitter.submitTopology("slotQueueCounters", conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("slotQueueCounters", conf, builder.createTopology());
            /*Utils.sleep(10000);
            cluster.killTopology("slotQueueCounters");
            cluster.shutdown();*/
        }
    }
}
