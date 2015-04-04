package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.ml.als.ALSWRFactorizer;
import com.innometrics.integration.app.recommender.ml.model.ResultPreference;
import com.innometrics.integration.app.recommender.repository.TrainingDataModel;
import com.innometrics.integration.app.recommender.repository.impl.InMemoryTrainingDataModel;
import com.innometrics.utils.app.commons.settings.store.AppContextSettings;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class CalculationBolt extends AbstractRichBolt {
    private static final Logger LOG = Logger.getLogger(CalculationBolt.class);
    private static final long ITERATION_LENGTH = 5000L;
    private transient Recommender recommender;
    private LinkedTransferQueue<Tuple> calculationQueue = new LinkedTransferQueue<>();
    private LinkedTransferQueue<ImmutablePair<Tuple, ResultPreference>> qeQueue = new LinkedTransferQueue<>();
    private LinkedTransferQueue<ResultPreference> defaultQueue = new LinkedTransferQueue<>();
    private AppContextSettings configuration;
    private TrainingDataModel trainingDataModel;

    @Override
    public void init() {
        try {
            this.trainingDataModel = new InMemoryTrainingDataModel(50000);
            this.recommender = getRecommender();
            this.configuration = new AppContextSettings();
            new Thread(new RecommendationRunner()).start();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private Recommender getRecommender() throws TasteException {
        if (this.recommender == null) {
            this.recommender = new SVDRecommender(trainingDataModel.getDataModel(), new ALSWRFactorizer(trainingDataModel.getDataModel(), 10, 0.01, 5));
        }
        return this.recommender;
    }


    @Override
    public void execute(Tuple tuple) {
        if (calculationQueue.size() > BATCH_LIMIT) {
            try {
                Thread.sleep(ITERATION_LENGTH / 100);
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
        registerPreference(tuple);
        for (ResultPreference eachResult : drainQueue(defaultQueue)) {
            getOutputCollector().emit(DEFAULT_STREAM, new Values(eachResult));
        }
        for (Pair<Tuple, ResultPreference> eachResult : drainQueue(qeQueue)) {
            Preference eachPreference = (Preference) eachResult.getLeft().getValueByField(PREFERENCE);
            getOutputCollector().emit(QE_STREAM, eachResult.getLeft(), new Values(getTopologyContext().getThisTaskIndex(), eachPreference, eachResult.getRight()));
            getOutputCollector().ack(eachResult.getLeft());
        }
    }

    private <T> Collection<T> drainQueue(BlockingQueue<T> toDrain) {
        Collection<T> toReturn = new ArrayList<>();
        toDrain.drainTo(toReturn);
        return toReturn;
    }


    private void registerPreference(Tuple preference) {
        calculationQueue.put(preference);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DEFAULT_STREAM, new Fields(ESTIMATION));
        outputFieldsDeclarer.declareStream(QE_STREAM, new Fields(BOLT_IDX, PREFERENCE, ESTIMATION));
    }

    private class RecommendationRunner implements Runnable {


        @Override
        public void run() {

            while (true) {
                Collection<Tuple> batch = drainQueue(calculationQueue);
                if (batch.size() > 0) {
                    try {
                        if (batch.size() < BATCH_LIMIT / 10) {
                            Thread.sleep(ITERATION_LENGTH);
                            batch.addAll(drainQueue(calculationQueue));
                        }
                        LOG.info("Running calculation on " + batch.size() + " new items.");
                        for (Tuple tuple : batch) {
                            trainingDataModel.setPreference((Preference) tuple.getValueByField(PREFERENCE));
                        }
                        getRecommender().refresh(new ArrayList<Refreshable>());
                        Set<Long> uids = new HashSet<>();
                        for (Tuple tuple : batch) {
                            Preference eachPreference = (Preference) tuple.getValueByField(PREFERENCE);
                            try {
                                long uid = eachPreference.getUserID();
                                if (!uids.contains(uid)) {
                                    List<RecommendedItem> results = getRecommender().recommend(eachPreference.getUserID(), 10);
                                    for (RecommendedItem eachItem : results) {
                                        defaultQueue.put(new ResultPreference(uid, eachItem.getItemID(), eachItem.getValue()));
                                    }
                                    uids.add(uid);
                                }
                                qeQueue.put(new ImmutablePair<>(
                                        tuple,
                                        new ResultPreference(
                                                eachPreference.getUserID()
                                                , eachPreference.getItemID(),
                                                getRecommender().estimatePreference(eachPreference.getUserID(), eachPreference.getItemID()))));
                            } catch (NoSuchUserException | NoSuchItemException e) {
                                LOG.info("No recommendation for " + eachPreference);
                            }
                        }
                    } catch (TasteException | InterruptedException e) {
                        LOG.warn(e);
                    }
                } else {
                    try {
                        Thread.sleep(ITERATION_LENGTH);
                        LOG.info("No new preference:" + new Date());
                    } catch (InterruptedException e) {
                        LOG.error(e);
                    }
                }
            }
        }
    }
}
