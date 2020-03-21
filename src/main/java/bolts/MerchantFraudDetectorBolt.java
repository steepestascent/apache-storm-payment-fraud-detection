package bolts;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class MerchantFraudDetectorBolt extends ShellBolt implements IRichBolt {

    public MerchantFraudDetectorBolt() {
        super("python", "merchant_fraud_detector.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("merchantAlertStream",
                new Fields("accountId", "merchantId", "transactionAmount", "boltName"));
        declarer.declareStream("merchantGlobalStream",
                new Fields("accountId", "merchantId", "transactionAmount", "allowAlert"));
        declarer.declareStream("merchantStatsStream",
                new Fields(
                        "merchantId",
                        "accountId",
                        "numberOfTransactions",
                        "transactionAmount",
                        "mean",
                        "std",
                        "minimum",
                        "maximum",
                        "currentValueAndHistoricalMinimumValue"
                ));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
