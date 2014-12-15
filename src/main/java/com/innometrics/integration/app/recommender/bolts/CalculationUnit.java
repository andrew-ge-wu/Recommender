package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.repository.CSVTrainingDataModel;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class CalculationUnit extends BaseRichBolt {
    private Map config;
    private OutputCollector outputCollector;
    private TopologyContext topologyContext;
    private Recommender recommender;
    private long nextRun;
    private static long ITERATION_LENGTH = 1000L;
    private ConcurrentSkipListSet<Long> uid = new ConcurrentSkipListSet<Long>();
    private CSVTrainingDataModel trainingDataModel;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.topologyContext = topologyContext;
        this.config = map;
        try {
            this.trainingDataModel = new CSVTrainingDataModel(new File(topologyContext.getThisComponentId() + "-" + topologyContext.getThisTaskIndex() + ".data"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Recommender getRecommender() throws TasteException {
        if (this.recommender == null) {
            this.recommender = new SVDRecommender(trainingDataModel.getDataModel(), new ALSWRFactorizer(trainingDataModel.getDataModel(), 3, 0.1, 10));

        }
        return this.recommender;
    }

    @Override
    public void execute(Tuple tuple) {
        // System.out.println(topologyContext.getThisComponentId() + "-" + topologyContext.getThisTaskIndex() + " received:" + tuple.getValues());
        long userId = Long.parseLong(tuple.getStringByField(USER_ID));
        long itemId = Long.parseLong(tuple.getStringByField(ITEM_ID));
        float preference = Float.parseFloat(tuple.getStringByField(PREFERENCE));
        long timestamp = Long.parseLong(tuple.getStringByField(TIMESTAMP));
        try {
            trainingDataModel.setPreference(userId, itemId, preference, timestamp);
            uid.add(userId);
            if (nextRun == 0) setNextRun();
            if (nextRun < System.currentTimeMillis()) {
                System.out.println("Running recommendation...\t");
                long duration = runRecommendation(tuple);
                System.out.println("finished in " + duration + "ms");
                setNextRun();
            }
            outputCollector.ack(tuple);
        } catch (TasteException e) {
            System.err.println(e.getMessage());
            outputCollector.ack(tuple);
        }
    }

    private long runRecommendation(Tuple anchor) throws TasteException {
        long start = System.currentTimeMillis();
        getRecommender().refresh(null);
        for (long eachUid : uid) {
            List<RecommendedItem> results = getRecommender().recommend(eachUid, 10);
            for (RecommendedItem eachItem : results) {
                System.out.println(eachUid + ":" + eachItem.getItemID() + ":" + eachItem.getValue());
                outputCollector.emit(anchor, new Values(eachUid, eachItem.getItemID(), eachItem.getValue()));
            }
        }
        uid.clear();
        return System.currentTimeMillis() - start;
    }

    private void setNextRun() {
        nextRun = System.currentTimeMillis() + ITERATION_LENGTH;
        System.out.println("Next recommendation run at:" + new Date(nextRun));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(USER_ID, ITEM_ID, PREFERENCE));
    }
}
