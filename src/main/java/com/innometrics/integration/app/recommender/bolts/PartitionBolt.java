package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.ml.partition.PartitionLogic;
import com.innometrics.integration.app.recommender.ml.partition.impl.RandomPartitionLogic;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.model.Preference;

import java.util.Map;

import static com.innometrics.integration.app.recommender.utils.Constants.PARTITION_ID;
import static com.innometrics.integration.app.recommender.utils.Constants.PREFERENCE;

/**
 * @author andrew, Innometrics
 */
public class PartitionBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(PartitionBolt.class);
    private OutputCollector outputCollector;
    private TopologyContext topologyContext;
    private Map config;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.topologyContext = topologyContext;
        this.config = map;
    }

    @Override
    public void execute(Tuple tuple) {
        Preference preference = (Preference) tuple.getValueByField(PREFERENCE);
        outputCollector.emit(tuple, new Values(getPartitionLogic().getPartitionString(preference), preference));
        outputCollector.ack(tuple);
    }

    private PartitionLogic getPartitionLogic() {
        return new RandomPartitionLogic();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(PARTITION_ID, PREFERENCE));
    }
}
