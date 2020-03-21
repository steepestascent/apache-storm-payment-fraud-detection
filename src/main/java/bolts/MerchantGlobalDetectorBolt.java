package bolts;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class MerchantGlobalDetectorBolt extends ShellBolt implements IRichBolt {

    public MerchantGlobalDetectorBolt() {
        super("python", "merchant_global_fraud_detector.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("merchantGlobalAlertStream", new Fields("accountId", "merchantId", "transactionAmount", "boltName"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
