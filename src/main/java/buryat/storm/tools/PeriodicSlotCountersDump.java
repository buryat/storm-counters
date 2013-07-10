package buryat.storm.tools;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.*;

public final class PeriodicSlotCountersDump<A, B> extends TimerTask {
    private SlotCounters<A, B> counters;
    private JedisPool jedisPool;
    private int redis_db;
    private String prefix = "";

    public PeriodicSlotCountersDump(SlotCounters<A, B> counters, JedisPool jedisPool, int redis_db) {
        this.counters = counters;
        this.jedisPool = jedisPool;
        this.redis_db = redis_db;
    }
    public PeriodicSlotCountersDump(SlotCounters<A, B> counters, JedisPool jedisPool, int redis_db, String prefix) {
        this.counters = counters;
        this.jedisPool = jedisPool;
        this.redis_db = redis_db;
        this.prefix = prefix;
    }

    public void run() {
        dump();
    }

    private void dump() {
        Map<A, Map<B, Long>> slots = counters.getSlots();

        Jedis redis = jedisPool.getResource();
        Transaction multi = redis.multi();
        multi.select(redis_db);

        for (A slot : slots.keySet()) {
            Map<B, Long> keys = slots.remove(slot);

            for (B key : keys.keySet()) {
                Long v = keys.remove(key);

                multi.hincrBy(prefix + key.toString(), slot.toString(), v);
            }
        }

        multi.exec();

        jedisPool.returnResource(redis);
    }
}
