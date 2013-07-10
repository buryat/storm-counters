package buryat.storm.tools;

import org.junit.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

@SuppressWarnings("unchecked")
public class PeriodicSlotCountersDumpTest {
    @Test
    public void dumpTest() {
        JedisPool jedisPool = new JedisPool("localhost", 6379);

        SlotCounters counters = new SlotCounters<String, Integer>();
        int minute = (int) (new Date().getTime() / 1000 / 60);

        counters.incr("test", minute);
        counters.incr("test", minute);
        counters.incr("test", minute);

        TimerTask dump = new PeriodicSlotCountersDump(counters, jedisPool, 0);
        dump.run();

        Jedis jedis = jedisPool.getResource();

        Assert.assertEquals(jedis.hget("test", String.valueOf(minute)), "3");

        jedis.del("test");
    }

    @Test
    public void dictionaryZSETTest() {
        JedisPool jedisPool = new JedisPool("localhost");

        SlotCounters counters = new SlotCounters<String, Integer>();

        counters.incr("test", 0);
        counters.incr("test", 0);
        counters.incr("test", 0);

        counters.incr("test.test1.test2.test3", 0);
        counters.incr("test.test1.test2.test3", 0);
        counters.incr("test.test1.test2.test3", 0);

        TimerTask dump = new PeriodicSlotCountersDumpZSET(counters, jedisPool, 0, "test:", true);
        dump.run();

        Jedis jedis = jedisPool.getResource();

        Assert.assertEquals(jedis.zscore("dic", "test"), Double.valueOf(3));
        Assert.assertEquals(jedis.zscore("dic:test.test1.test2", "test3"), Double.valueOf(3));

        jedis.del("test:test", "test:test.test1.test2.test3", "dic", "dic:test.test1.test2");
    }

    @Test
    public void dictionaryTest() {
        JedisPool jedisPool = new JedisPool("localhost");

        SlotCounters counters = new SlotCounters<String, Integer>();

        counters.incr("test", 0);
        counters.incr("test", 0);
        counters.incr("test", 0);

        counters.incr("test.test1.test2.test3", 0);
        counters.incr("test.test1.test2.test3", 0);
        counters.incr("test.test1.test2.test3", 0);

        TimerTask dump = new PeriodicSlotCountersDump(counters, jedisPool, 0, "test:", true);
        dump.run();

        Jedis jedis = jedisPool.getResource();

        Assert.assertTrue(jedis.sismember("dic", "test"));
        Assert.assertTrue(jedis.sismember("dic:test.test1.test2", "test3"));

        jedis.del("test:test", "test:test.test1.test2.test3", "dic", "dic:test.test1.test2");
    }
}
