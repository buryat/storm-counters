package buryat.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import buryat.storm.bolt.SlotCountBolt;
import buryat.storm.spout.RedisQueueSpout;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SlotCountersTopology {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(
                "queue",
                new RedisQueueSpout(properties),
                4
        );
        builder.setBolt("count", new SlotCountBolt(properties), 4).shuffleGrouping("queue");

        Config conf = new Config();

        if (args.length == 1) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("slotCounters", conf, builder.createTopology());

            /*Utils.sleep(10000);
            cluster.killTopology("slotCounters");
            cluster.shutdown();*/
        }
    }
}
