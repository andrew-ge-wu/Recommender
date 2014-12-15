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

import java.util.Map;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class PartitionBolt extends BaseRichBolt {
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
      //  System.out.println(topologyContext.getThisComponentId() + "-" + topologyContext.getThisTaskIndex() + " received:" + tuple.getValues());
        String userId = tuple.getStringByField(USER_ID);
        String itemId = tuple.getStringByField(ITEM_ID);
        String preference = tuple.getStringByField(PREFERENCE);
        String timestamp = tuple.getStringByField(TIMESTAMP);
        outputCollector.emit(tuple, new Values(getPartitionLogic().getPartitionString(userId, itemId, Long.parseLong(timestamp)), userId, itemId, preference, timestamp));
        outputCollector.ack(tuple);
    }

    private PartitionLogic getPartitionLogic() {
        return new RandomPartitionLogic();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(PARTITION_ID, USER_ID, ITEM_ID, PREFERENCE, TIMESTAMP));
    }
}
