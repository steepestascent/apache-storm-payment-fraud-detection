package bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class MerchantStatsBolt extends BaseBasicBolt {

    private FileOutputStream outputStream = null;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        super.prepare(topoConf, context);
        try {
            this.outputStream = new FileOutputStream("merchant-stats.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String merchantId = tuple.getString(0);
        String accountId = tuple.getString(1);
        long numberOfTransactions = tuple.getLong(2);
        double transactionAmount = tuple.getDouble(3);
        double mean = tuple.getDouble(4);
        double std = tuple.getDouble(5);
        double minimum = tuple.getDouble(6);
        double maximum = tuple.getDouble(7);
        double ratio = tuple.getDouble(8);

        try {
            this.outputStream.write(
                    String.format("merchantId: %s\taccountId: %s\tcount: %.2f\tamount: %.2f\tmean: %.2f\t" +
                                    "std: %.2f\tmin: %.2f\tmax: %.2f\tratio: %.2f\n",
                            merchantId,
                            accountId,
                            (double)numberOfTransactions,
                            transactionAmount,
                            mean,
                            std,
                            minimum,
                            maximum,
                            ratio).getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void finalize()
    {
        try {
            this.outputStream.flush();
            this.outputStream.close();
            super.finalize();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /* nothing to declare */
    }
}
