package buryat.storm.tools;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;

import java.util.List;
import java.util.Iterator;

import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;


public final class AWSRedises extends TimerTask {
    private AmazonAWS aws;
    private int redis_port;
    private JedisPoolConfig jedisPoolConfig;

    private Iterator<String> hostIterator;
    private Map<String, JedisPool> pools = new ConcurrentHashMap<String, JedisPool>();

    public AWSRedises(String key, String secret, String[] elbs, int redis_port) {
        this.redis_port = redis_port;
        this.jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setTestOnBorrow(true);

        aws = new AmazonAWS(key, secret);
        aws.setElbs(elbs);
    }

    public void run() {
        List<String> hosts = aws.getInstances();

        for (String host : pools.keySet()) {
            if (!hosts.contains(host)) {
                remove(host);
            }
        }

        for (String host : hosts) {
            if (pools.get(host) == null) {
                JedisPool pool = new JedisPool(jedisPoolConfig, host, redis_port);

                pools.put(host, pool);
            }
        }

        hostIterator = pools.keySet().iterator();
    }

    public boolean cancel() {
        for (String host : pools.keySet()) {
            pools.get(host).destroy();
        }

        return super.cancel();
    }

    public JedisPool get() {
        if (hostIterator.hasNext()) {
            String host = hostIterator.next();

            return pools.get(host);
        } else {
            if (pools.size() == 0) {
                return null;
            }
            hostIterator = pools.keySet().iterator();
            return get();
        }
    }

    public void remove(String host) {
        pools.get(host).destroy();
        pools.remove(host);
    }
}