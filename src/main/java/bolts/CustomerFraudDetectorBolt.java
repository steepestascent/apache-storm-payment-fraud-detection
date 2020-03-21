package bolts;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class CustomerFraudDetectorBolt extends ShellBolt implements IRichBolt {

    public CustomerFraudDetectorBolt() {
        super("python", "customer_fraud_detector.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("customerAlertStream",
                new Fields("accountId", "merchantId", "transactionAmount", "boltName"));
        declarer.declareStream("customerGlobalStream",
                new Fields("accountId", "merchantId", "transactionAmount", "allowAlert"));
        declarer.declareStream("customerStatsStream",
                new Fields(
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
