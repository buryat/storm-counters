package buryat.storm.tools;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class SlotCounters<A, B> implements Serializable {
    private static final long serialVersionUID = 1239238749087211L;

    private class MutableLong {
        long value = 1;
        void incr() { ++value; }
        long get() {return value;}
    }

    private final Map<A, Map<B, MutableLong>> counters = new ConcurrentHashMap<A, Map<B, MutableLong>>();

    public Map<B, Long> getSlot(A slot) {
        Map<B, MutableLong> _slot = counters.remove(slot);
        if (_slot == null) {
            return new ConcurrentHashMap<B, Long>();
        }

        Map<B, Long> result = new ConcurrentHashMap<B, Long>();

        for (B key : _slot.keySet()) {
            result.put(key, _slot.get(key).get());
        }

        return result;
    }

    public Map<A, Map<B, Long>> getSlots() {
        Map<A, Map<B, Long>> result = new ConcurrentHashMap<A, Map<B, Long>>();

        for (A slot : counters.keySet()) {
            result.put(slot, getSlot(slot));
        }

        return result;
    }

    public void incr(A slot, B key) {
        Map<B, MutableLong> _slot = counters.get(slot);
        if (_slot == null) {
            _slot = new ConcurrentHashMap<B, MutableLong>();
            _slot.put(key, new MutableLong());
            counters.put(slot, _slot);
        } else {
            MutableLong value = _slot.get(key);
            if (value == null) {
                _slot.put(key, new MutableLong());
            } else {
                value.incr();
            }
        }
    }
}