package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.ml.partition.PartitionLogic;
import com.innometrics.integration.app.recommender.ml.partition.impl.RandomPartitionLogic;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.model.Preference;

import static com.innometrics.integration.app.recommender.utils.Constants.PARTITION_ID;
import static com.innometrics.integration.app.recommender.utils.Constants.PREFERENCE;

/**
 * @author andrew, Innometrics
 */
public class PartitionBolt extends AbstractRichBolt {
    private static final Logger LOG = Logger.getLogger(PartitionBolt.class);

    @Override
    public void execute(Tuple tuple) {
        Preference preference = (Preference) tuple.getValueByField(PREFERENCE);
        getOutputCollector().emit(tuple, new Values(getPartitionLogic().getPartitionString(preference), preference));
        getOutputCollector().ack(tuple);
    }

    private PartitionLogic getPartitionLogic() {
        return new RandomPartitionLogic();
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(PARTITION_ID, PREFERENCE));
    }
}
