package com.innometrics.integration.app.recommender.spouts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public abstract class AbstractLearningSpout extends BaseRichSpout {


    @Override
    public final void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(USER_ID, ITEM_ID, PREFERENCE, TIMESTAMP));
    }
}
