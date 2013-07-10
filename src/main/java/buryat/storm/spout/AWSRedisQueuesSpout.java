package buryat.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import buryat.storm.tools.AWSRedises;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;

public class AWSRedisQueuesSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    private final Properties props;

    private String redis_queue_key;
    private int redis_queue_iter_size;

    private AWSRedises redises;
    private int redis_db;

    public AWSRedisQueuesSpout(Properties props) {
        this.props = props;

        this.redis_queue_key = props.getProperty("redis.queue.key");
        this.redis_queue_iter_size = Integer.valueOf(props.getProperty("redis.queue.size"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

        this.redis_db = Integer.valueOf(props.getProperty("redis.db"));

        redises = new AWSRedises(
                props.getProperty("aws.key"),
                props.getProperty("aws.secret"),
                props.getProperty("aws.elbs").split(","),
                Integer.valueOf(props.getProperty("redis.port"))
        );
        redises.run();

        Integer refresh_rate = Integer.valueOf(props.getProperty("aws.refresh_rate")) * 1000;

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(redises, refresh_rate, refresh_rate);
    }

    @Override
    public void close() {
        redises.cancel();
    }

    @Override
    public void nextTuple() {
        JedisPool jedisPool = redises.get();

        Jedis redis = jedisPool.getResource();

        try {
            redis.getClient().setTimeoutInfinite();

            redis.select(redis_db);

            Transaction multi = redis.multi();
            Response<List<String>> ret = multi.lrange(redis_queue_key, 0, redis_queue_iter_size - 1);
            multi.ltrim(redis_queue_key, redis_queue_iter_size, -1);
            multi.exec();

            jedisPool.returnResource(redis);

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
        } catch (JedisConnectionException e) {
            jedisPool.returnBrokenResource(redis);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "item"));
    }
}