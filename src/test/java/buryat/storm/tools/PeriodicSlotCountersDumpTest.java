package buryat.storm.tools;

import org.junit.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class PeriodicSlotCountersDumpTest {
    @Test
    public void dumpTest() {
        JedisPool jedisPool = new JedisPool("localhost", 6379);

        SlotCounters<Integer, String> counters = new SlotCounters<Integer, String>();
        int minute = (int) (new Date().getTime() / 1000 / 60);

        counters.incr(minute, "test");
        counters.incr(minute, "test");
        counters.incr(minute, "test");

        TimerTask dump = new PeriodicSlotCountersDump<Integer, String>(counters, jedisPool, 0);
        dump.run();

        Jedis jedis = jedisPool.getResource();

        Assert.assertEquals(jedis.hget("test", String.valueOf(minute)), "3");

        jedis.del("test");
    }
}
