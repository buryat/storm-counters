package buryat.storm.tools;

import org.junit.*;
import redis.clients.jedis.Jedis;

public class AWSRedisesTests {
    @Test
    public void redisesTest() {
        AWSRedises redises = new AWSRedises(
                "key",
                "secret",
                new String[]{"elb1", "elb2"},
                6379
        );
        redises.run();

        for (int i = 0; i < 10; i++) {
            for (String key : redises.get().getResource().keys("*")) {
                System.out.println(key);
            }
        }
    }
}
