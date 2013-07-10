package buryat.storm.tools;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class PeriodicDictionaryDumpTest {
    @Test
    public void dumpTest() {
        JedisPool jedisPool = new JedisPool("localhost", 6379);

        Map<String, Boolean> dictionary = new ConcurrentHashMap<String, Boolean>();
        dictionary.put("test", true);
        dictionary.put("test.test", true);
        dictionary.put("test.test.test", true);

        TimerTask dump = new PeriodicDictionaryDump(dictionary, jedisPool, 0);
        dump.run();

        Jedis jedis = jedisPool.getResource();

        Assert.assertTrue(jedis.sismember("dic", "test"));
        Assert.assertTrue(jedis.sismember("dic:test", "test"));
        Assert.assertTrue(jedis.sismember("dic:test.test", "test"));

        jedis.del("dic", "dic:test", "dic:test.test");
    }
}
