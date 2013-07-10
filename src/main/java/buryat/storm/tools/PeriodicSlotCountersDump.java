package buryat.storm.tools;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.*;

@SuppressWarnings("unchecked")
public final class PeriodicSlotCountersDump extends TimerTask {
    private SlotCounters counters;
    private JedisPool jedisPool;
    private int redis_db;
    private String prefix = "";
    private boolean dictionary = false;

    public PeriodicSlotCountersDump(SlotCounters counters, JedisPool jedisPool, int redis_db) {
        this.counters = counters;
        this.jedisPool = jedisPool;
        this.redis_db = redis_db;
    }
    public PeriodicSlotCountersDump(SlotCounters counters, JedisPool jedisPool, int redis_db, String prefix) {
        this.counters = counters;
        this.jedisPool = jedisPool;
        this.redis_db = redis_db;
        this.prefix = prefix;
    }
    public PeriodicSlotCountersDump(SlotCounters counters, JedisPool jedisPool, int redis_db, String prefix, boolean dictionary) {
        this.counters = counters;
        this.jedisPool = jedisPool;
        this.redis_db = redis_db;
        this.prefix = prefix;
        this.dictionary = dictionary;
    }

    public void run() {
        dumper();
    }

    private void dumper() {
        Map<String, Map<Integer, Long>> keys = counters.getSlots();

        Jedis redis = jedisPool.getResource();
        Transaction multi = redis.multi();
        multi.select(redis_db);

        for (String key : keys.keySet()) {
            Map<Integer, Long> slots = keys.get(key);

            for (Integer slot : slots.keySet()) {
                multi.hincrBy(prefix + key, slot.toString(), slots.get(slot));
            }

            if (dictionary) {
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
            }
        }

        multi.exec();

        jedisPool.returnResource(redis);
    }
}
