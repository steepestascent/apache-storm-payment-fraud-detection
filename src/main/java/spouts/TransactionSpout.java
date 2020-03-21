package spouts;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class TransactionSpout extends BaseRichSpout {
    private static final int TEN_MINUTES = 600000;
    private SpoutOutputCollector _collector;
    private List<String> transactions;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        try {
            transactions = IOUtils.readLines(
                    ClassLoader.getSystemResourceAsStream("data/resources/data_small.csv"),
                    Charset.defaultCharset().name());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        for (String t : transactions) {
            Utils.sleep(50);
            _collector.emit(new Values(t));
        }

        System.out.println("No more transactions. Waiting until timeout.");
        Utils.sleep(TEN_MINUTES);
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transaction"));
    }

}
