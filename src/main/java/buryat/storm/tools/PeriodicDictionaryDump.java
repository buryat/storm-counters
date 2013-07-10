package buryat.storm.tools;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.Map;
import java.util.TimerTask;

@SuppressWarnings("unchecked")
public final class PeriodicDictionaryDump extends TimerTask {
    private Map<String, Boolean> dictionary;
    private JedisPool jedisPool;
    private int redis_db;

    public PeriodicDictionaryDump(Map dictionary, JedisPool jedisPool, int redis_db) {
        this.dictionary = dictionary;
        this.jedisPool = jedisPool;
        this.redis_db = redis_db;
    }

    public void run() {
        dump();
    }

    private void dump() {
        Jedis redis = jedisPool.getResource();
        Transaction multi = redis.multi();
        multi.select(redis_db);

        for (String key : dictionary.keySet()) {
            String[] parts = key.split("\\.");

            String el = parts[parts.length - 1];
            String k;

            if (parts.length == 1) {
                k = "dic";
            } else {
                k = "dic:" + parts[0];
                for (int i = 1; i < parts.length - 1; i++) {
                    k += "." + parts[i];
                }
            }

            multi.sadd(k, el);

            dictionary.remove(key);
        }

        multi.exec();

        jedisPool.returnResource(redis);
    }
}
