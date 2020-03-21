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

public class AlertBolt extends BaseBasicBolt {

    private FileOutputStream outputStream = null;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        super.prepare(topoConf, context);
        try {
            this.outputStream = new FileOutputStream("alerts.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String accountId = tuple.getString(0);
        String merchant = tuple.getString(1);
        double transactionAmount = tuple.getDouble(2);
        String boltName = tuple.getString(3);

        try {
            outputStream.write(
                    String.format("boltName: %-29s accountId: %s\tmerchantId: %s\tamount: %.2f\n",
                            boltName, accountId, merchant, transactionAmount).getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void finalize()
    {
        try {
            outputStream.flush();
            outputStream.close();
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
