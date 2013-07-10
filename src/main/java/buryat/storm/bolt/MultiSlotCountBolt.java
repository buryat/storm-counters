package buryat.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import buryat.storm.tools.PeriodicSlotCountersDump;
import buryat.storm.tools.SlotCounters;
import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

public class MultiSlotCountBolt extends BaseRichBolt {
    OutputCollector _collector;

    private static final Logger LOG = Logger.getLogger(CountBolt.class);

    private final int dumpers;
    private final int[] countIntervals;
    private final SlotCounters<Integer, String>[] slotCounters;
    private final Properties props;

    @SuppressWarnings("unchecked")
    public MultiSlotCountBolt(Properties props) {
        this.props = props;

        dumpers = Integer.valueOf(props.getProperty("dumpers"));

        countIntervals = new int[dumpers];
        slotCounters = (SlotCounters<Integer, String>[]) new SlotCounters[dumpers];
        for (int i = 0; i < dumpers; i++) {
            slotCounters[i] = new SlotCounters<Integer, String>();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        String redis_host = props.getProperty("redis_dump.host");
        int redis_port = Integer.valueOf(props.getProperty("redis_dump.port"));
        int redis_db = Integer.valueOf(props.getProperty("redis_dump.db"));

        JedisPool jedisPool = new JedisPool(
                new JedisPoolConfig(),
                redis_host,
                redis_port
        );

        for (int i = 0; i < dumpers; i++) {
            String dumper = "dumper[" + i + "].";

            String key_prefix = props.getProperty(dumper + "prefix");
            int count_interval = Integer.valueOf(props.getProperty(dumper + "count_interval"));
            int dump_interval = Integer.valueOf(props.getProperty(dumper + "dump_interval")) * 1000;

            countIntervals[i] = count_interval;

            TimerTask dump = new PeriodicSlotCountersDump<Integer, String>(
                    slotCounters[i],
                    jedisPool,
                    redis_db,
                    key_prefix
            );

            Timer timer = new Timer();
            timer.scheduleAtFixedRate(dump, dump_interval, dump_interval);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Integer timestamp = tuple.getInteger(0);
        String key = tuple.getString(1);

        for (int i = 0; i < dumpers; i++) {
            int time = ((int) timestamp / countIntervals[i]) * countIntervals[i];

            slotCounters[i].incr(time, key);
        }

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count"));
    }
}
