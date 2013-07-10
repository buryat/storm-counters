package buryat.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.apache.log4j.Logger;

import java.util.*;

import buryat.storm.tools.SlotCounters;
import buryat.storm.tools.PeriodicSlotCountersDump;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class SlotCountBolt extends BaseRichBolt {
    OutputCollector _collector;

    private static final Logger LOG = Logger.getLogger(CountBolt.class);

    private final SlotCounters<String, Integer> counters;
    private final Properties props;

    public SlotCountBolt(Properties props) {
        this.props = props;
        counters = new SlotCounters<String, Integer>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        JedisPool jedisPool = new JedisPool(
                new JedisPoolConfig(),
                props.getProperty("redis_dump.host"),
                Integer.valueOf(props.getProperty("redis_dump.port"))
        );

        TimerTask dump = new PeriodicSlotCountersDump(
                counters,
                jedisPool,
                Integer.valueOf(props.getProperty("redis_dump.db"))
        );

        Integer dump_rate = Integer.valueOf(props.getProperty("redis_dump.rate")) * 1000;

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(dump, dump_rate, dump_rate);
    }

    @Override
    public void execute(Tuple tuple) {
        Integer minute = tuple.getInteger(0);
        String key = tuple.getString(1);

        minute = ((int) minute / 60) * 60;

        counters.incr(key, minute);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
