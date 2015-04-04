package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.ml.partition.PartitionLogic;
import com.innometrics.integration.app.recommender.ml.partition.impl.UserPartitionLogic;
import com.innometrics.integration.app.recommender.utils.Constants;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.model.Preference;

import static com.innometrics.integration.app.recommender.utils.Constants.PREFERENCE;

/**
 * @author andrew, Innometrics
 */
public class PartitionBolt extends AbstractRichBolt {
    private static final Logger LOG = Logger.getLogger(PartitionBolt.class);
    private transient PartitionLogic partitionLogic;

    @Override
    public void execute(Tuple tuple) {
        Preference preference = (Preference) tuple.getValueByField(PREFERENCE);
        for (int i = 0; i < Constants.NR_CROSS_REF; i++) {
            String[] partition = getPartitionLogic().getPartitionStrings(preference,i);
            Object[] toSend = new Object[partition.length + 1];
            System.arraycopy(partition, 0, toSend, 0, partition.length);
            toSend[toSend.length - 1] = preference;
            getOutputCollector().emit(tuple, new Values(toSend));
        }
        getOutputCollector().ack(tuple);
    }

    private PartitionLogic getPartitionLogic() {
        if (partitionLogic == null) {
            partitionLogic = new UserPartitionLogic();
        }
        return partitionLogic;
    }

    public String[] groupingFields() {
        return getPartitionLogic().groupingFields();
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields((String[]) ArrayUtils.add(groupingFields(), PREFERENCE)));
    }
}
