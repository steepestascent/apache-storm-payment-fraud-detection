package bolts;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class CleanDataBolt extends ShellBolt implements IRichBolt {

    public CleanDataBolt() {
        super("python", "clean_data.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("customerDetectorStream", new Fields("accountId", "merchantId", "transactionAmount"));
        declarer.declareStream("merchantDetectorStream", new Fields("accountId", "merchantId", "transactionAmount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
