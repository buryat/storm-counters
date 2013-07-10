package buryat.storm.bolt;

import backtype.storm.topology.base.BaseRichBolt;
import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import buryat.storm.tools.Counters;

import java.util.Date;
import java.util.Map;

public class CountBolt extends BaseRichBolt {
    OutputCollector _collector;

    private static final Logger LOG = Logger.getLogger(CountBolt.class);

    private final Counters<String> counter;
    private long prev_time = new Date().getTime();

    public CountBolt() {
        counter = new Counters<String>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getString(0);
        counter.incr(key);

        System.out.print(counter);
        System.out.println("-----");

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
