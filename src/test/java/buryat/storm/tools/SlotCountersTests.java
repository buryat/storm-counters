package buryat.storm.tools;

import org.junit.*;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;

public class SlotCountersTests {
    private <A, B> void printMap(Map<A, Map<B, Long>> map) {
        for (A slot : map.keySet()) {
            System.out.println("slot " + slot + ":");

            Map<B, Long> keys = map.get(slot);

            for (B key : keys.keySet()) {
                Long val = keys.get(key);

                System.out.println("- " + key + " : " + val);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void basicTest() {
        SlotCounters counters = new SlotCounters<Integer, String>();
        int minute = (int) (new Date().getTime() / 1000 / 60);

        counters.incr(minute, "test");
        counters.incr(minute, "test");
        counters.incr(minute, "test");

        Map slots = counters.getSlots();
        printMap(slots);

        Map test = new HashMap<Integer, HashMap<String, Long>>();
        Map test2 = new HashMap<String, Long>();
        test2.put("test", Long.valueOf(3));
        test.put(minute, test2);

        Assert.assertEquals(
            test,
            slots
        );
    }
}
