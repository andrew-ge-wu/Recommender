package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.ml.model.ResultPreference;
import com.innometrics.integration.app.recommender.repository.TrainingDataModel;
import com.innometrics.integration.app.recommender.repository.impl.MysqlTrainingDataModel;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class CalculationBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(CalculationBolt.class);
    private OutputCollector outputCollector;
    private TopologyContext topologyContext;
    private Recommender recommender;
    private long nextRun;
    private static long ITERATION_LENGTH = 1000L;
    private ArrayBlockingQueue<Preference> bufferQueue = new ArrayBlockingQueue<>(5000);
    private TrainingDataModel trainingDataModel;
    private transient Executor executor;
    private boolean isRecommenderRunning = false;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.topologyContext = topologyContext;
        try {
            this.trainingDataModel = new MysqlTrainingDataModel(topologyContext.getThisComponentId() + topologyContext.getThisTaskIndex(), 10000, 60, TimeUnit.SECONDS);
        } catch (IOException | ClassNotFoundException | SQLException | TasteException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    private Recommender getRecommender() throws TasteException {
        if (this.recommender == null) {
            this.recommender = new SVDRecommender(trainingDataModel.getDataModel(), new ALSWRFactorizer(trainingDataModel.getDataModel(), 3, 0.1, 5));
        }
        return this.recommender;
    }

    private Executor getExecutor() {
        if (this.executor == null) {
            this.executor = Executors.newSingleThreadExecutor();
        }
        return this.executor;
    }

    @Override
    public void execute(Tuple tuple) {
        outputCollector.ack(tuple);
        registerPreference((Preference) tuple.getValueByField(PREFERENCE));
        if (nextRun == 0) setNextRun();
        if (!isRecommenderRunning) {
            Set<Pair<Long, Long>> batchHistory = new HashSet<>();
            for (Preference eachPreference : drainBuffer()) {
                trainingDataModel.setPreference(eachPreference);
                batchHistory.add(new ImmutablePair<>(eachPreference.getUserID(), eachPreference.getItemID()));
            }
            if (nextRun < System.currentTimeMillis()) {
                try {
                    LOG.info("Running recommendation...\t");
                    getExecutor().execute(new RecommendationRunner(outputCollector, batchHistory));
                } finally {
                    setNextRun();
                }
            }
        }
    }

    private Collection<Preference> drainBuffer() {
        Collection<Preference> toReturn = new ArrayList<>();
        int itemDrained = bufferQueue.drainTo(toReturn);
        if (itemDrained > 10) {
            LOG.info("Drained " + itemDrained + " preferences from buff");
        }
        return toReturn;
    }

    private void registerPreference(Preference preference) {
        try {
            bufferQueue.put(preference);
        } catch (InterruptedException e) {
            LOG.warn(e);
        }
    }

    private void setNextRun() {
        nextRun = System.currentTimeMillis() + ITERATION_LENGTH;
        LOG.info("Next recommendation on CU" + topologyContext.getThisTaskIndex() + " run at:" + new Date(nextRun));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DEFAULT_STREAM, new Fields(ESTIMATION));
        outputFieldsDeclarer.declareStream(QE_STREAM, new Fields(BOLT_IDX, PREFERENCE, ESTIMATION));
    }

    private class RecommendationRunner implements Runnable {
        private final Collection<Pair<Long, Long>> batch;
        private final OutputCollector collector;

        public RecommendationRunner(OutputCollector collector, Collection<Pair<Long, Long>> batch) {
            this.batch = batch;
            this.collector = collector;
        }

        @Override
        public void run() {
            try {
                isRecommenderRunning = true;
                getRecommender().refresh(new ArrayList<Refreshable>());
                for (Pair<Long, Long> eachPreference : batch) {
                    List<RecommendedItem> results = getRecommender().recommend(eachPreference.getKey(), 10);
                    for (RecommendedItem eachItem : results) {
                        ResultPreference result = new ResultPreference(eachPreference.getKey(), eachItem.getItemID(), eachItem.getValue());
                        collector.emit(DEFAULT_STREAM, new Values(result));
                    }
                    Preference preference = trainingDataModel.getPreference(eachPreference.getKey(), eachPreference.getValue());
                    if (preference.getValue() != Float.MIN_VALUE) {
                        try {
                            collector.emit(QE_STREAM, new Values(
                                    topologyContext.getThisTaskIndex(),
                                    preference,
                                    new ResultPreference(
                                            eachPreference.getKey()
                                            , eachPreference.getValue(),
                                            recommender.estimatePreference(eachPreference.getKey(), eachPreference.getValue()))));
                        } catch (NoSuchUserException | NoSuchItemException e) {
                            LOG.debug("No recommendation for " + eachPreference);
                        }
                    }
                }
            } catch (TasteException e) {
                LOG.warn(e);
            } finally {
                isRecommenderRunning = false;
            }
        }
    }
}
