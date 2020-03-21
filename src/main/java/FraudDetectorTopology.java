import bolts.*;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import spouts.TransactionSpout;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class FraudDetectorTopology extends ConfigurableTopology {
    private static final int TEN_MINUTES = 600000;

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new FraudDetectorTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        /* Spout to read transaction csv file */
        builder.setSpout("spout", new TransactionSpout(), 1);

        /* Processing bolts */
        builder.setBolt("clean", new CleanDataBolt(), 8).shuffleGrouping("spout");


        /* detecting via customer */
        builder.setBolt("customerDetector", new CustomerFraudDetectorBolt(), 12)
                .fieldsGrouping("clean", "customerDetectorStream", new Fields("accountId"));
        builder.setBolt("customerGlobalDetector", new CustomerGlobalDetectorBolt())
                .globalGrouping("customerDetector", "customerGlobalStream");

        /* detecting via merchant */
        builder.setBolt("merchantDetector", new MerchantFraudDetectorBolt(), 12)
                .fieldsGrouping("clean", "merchantDetectorStream", new Fields("merchantId"));
        builder.setBolt("merchantGlobalDetector", new MerchantGlobalDetectorBolt())
                .globalGrouping("merchantDetector", "merchantGlobalStream");

        /* Reporting bolts */
        builder.setBolt("customerStats", new CustomerStatsBolt())
                .globalGrouping("customerDetector", "customerStatsStream");
        builder.setBolt("merchantStats", new MerchantStatsBolt())
                .globalGrouping("merchantDetector", "merchantStatsStream");

        /* Alert Bolt */
        BoltDeclarer alertBolt = builder.setBolt("alert", new AlertBolt());
        alertBolt.globalGrouping("customerDetector", "customerAlertStream");
        alertBolt.globalGrouping("customerGlobalDetector", "customerGlobalAlertStream");
        alertBolt.globalGrouping("merchantDetector", "merchantAlertStream");
        alertBolt.globalGrouping("merchantGlobalDetector", "merchantGlobalAlertStream");

        /* configurations */
        conf.setDebug(false);
        conf.setNumWorkers(3);

        /* submit topology*/
        String topologyName = "FraudDetectorTopology";
        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        submit(topologyName, conf, builder);

        /* Letting the topology run for 10 minutes */
        Utils.sleep(TEN_MINUTES);
        return 0;
    }
}
