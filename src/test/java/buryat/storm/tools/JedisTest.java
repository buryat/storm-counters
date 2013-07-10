package buryat.storm.tools;

import org.junit.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisTest {
    @Test
    public void poolTest() {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, 0, new String(), 1);
        pool.getResource().set("test", "1");
    }
}
