package buryat.storm.tools;

import org.junit.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisTest {
    @Test
    public void poolTest() {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, 0);
        Jedis jedis = pool.getResource();

        jedis.set("test", "1");

        Assert.assertEquals(jedis.get("test"), "1");
        Assert.assertEquals(jedis.del("test"), Long.valueOf("1"));
    }
}
