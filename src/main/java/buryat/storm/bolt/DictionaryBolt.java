package buryat.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import buryat.storm.tools.PeriodicDictionaryDump;
import buryat.storm.tools.PeriodicSlotCountersDump;
import buryat.storm.tools.SlotCounters;
import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class DictionaryBolt extends BaseRichBolt {
    OutputCollector _collector;

    private static final Logger LOG = Logger.getLogger(CountBolt.class);

    private final Map<String, Boolean> dictionary;
    private final Properties props;

    @SuppressWarnings("unchecked")
    public DictionaryBolt(Properties props) {
        this.props = props;

        dictionary = new ConcurrentHashMap<String, Boolean>();
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

        int dump_interval = Integer.valueOf(props.getProperty("dictionary.dump_interval")) * 1000;

        TimerTask dump = new PeriodicDictionaryDump(
                dictionary,
                jedisPool,
                redis_db
        );

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(dump, dump_interval, dump_interval);
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getString(1);
        if (!dictionary.containsKey(key)) {
            dictionary.put(key, true);
        }

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
