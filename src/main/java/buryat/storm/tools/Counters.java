package buryat.storm.tools;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;

public final class Counters<T> implements Serializable {
    private static final long serialVersionUID = 1239238749087210L;

    class MutableLong {
        long value = 1;
        void incr() { ++value; }
        long get() {return value;}
    }

    private final Map<T, MutableLong> keys = new HashMap<T, MutableLong>();

    public void incr(T key) {
        MutableLong count = keys.get(key);
        if (count == null) {
            keys.put(key, new MutableLong());
        } else {
            count.incr();
        }
    }

    public long get(T key) {
        MutableLong count = keys.get(key);
        if (count == null) {
            return 0;
        } else {
            return count.get();
        }
    }

    public String toString() {
        String s = "";
        for (T key : keys.keySet()) {
            s += key + " - " + keys.get(key).get() + "\n";
        }
        return s;
    }
}