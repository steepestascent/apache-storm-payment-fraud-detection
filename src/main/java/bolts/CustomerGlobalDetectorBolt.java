package bolts;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class CustomerGlobalDetectorBolt extends ShellBolt implements IRichBolt {

    public CustomerGlobalDetectorBolt() {
        super("python", "customer_global_fraud_detector.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("customerGlobalAlertStream", new Fields("accountId", "merchantId", "transactionAmount", "boltName"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
