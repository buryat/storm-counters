package buryat.storm.spout;

import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.Properties;

import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RedisQueueSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    private String redis_host;
    private int redis_port;
    private int redis_db;
    private String redis_queue_key;
    private int redis_queue_iter_size;

    private JedisPool pool;

    public RedisQueueSpout(Properties props) {
        this.redis_host = props.getProperty("redis.host");
        this.redis_port = Integer.valueOf(props.getProperty("redis.port"));
        this.redis_db = Integer.valueOf(props.getProperty("redis.db"));
        this.redis_queue_key = props.getProperty("redis.queue.key");
        this.redis_queue_iter_size = Integer.valueOf(props.getProperty("redis.queue.size"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        pool = new JedisPool(redis_host, redis_port);
    }

    @Override
    public void close() {
        pool.destroy();
    }

    @Override
    public void nextTuple() {
        Jedis redis = pool.getResource();
        redis.select(redis_db);

        Transaction multi = redis.multi();

        Response<List<String>> ret = multi.lrange(redis_queue_key, 0, redis_queue_iter_size - 1);
        multi.ltrim(redis_queue_key, redis_queue_iter_size, -1);

        multi.exec();

        pool.returnResource(redis);

        if (ret == null) {
            Utils.sleep(500);
        } else {
            for (String val : ret.get()) {
                String[] parts = val.split(":", 2);

                if (parts.length == 2) {
                    _collector.emit(new Values(Integer.valueOf(parts[0]), parts[1]));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "item"));
    }
}