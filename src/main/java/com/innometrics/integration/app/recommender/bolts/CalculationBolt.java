package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.ml.model.ResultPreference;
import com.innometrics.integration.app.recommender.repository.TrainingDataModel;
import com.innometrics.integration.app.recommender.repository.impl.MysqlTrainingDataModel;
import org.apache.commons.configuration.ConfigurationException;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class CalculationBolt extends AbstractRichBolt {
    private static final Logger LOG = Logger.getLogger(CalculationBolt.class);
    private static final long ITERATION_LENGTH = 1000L;
    private transient Recommender recommender;
    private long nextRun;
    private ArrayBlockingQueue<Preference> bufferQueue = new ArrayBlockingQueue<>(20000);
    private TrainingDataModel trainingDataModel;
    private transient ExecutorService executor;
    private boolean isRecommenderRunning = false;

    @Override
    public void init() {
        try {
            this.trainingDataModel = new MysqlTrainingDataModel(getTopologyContext().getThisComponentId() + getTopologyContext().getThisTaskIndex(), 10000, 60, TimeUnit.SECONDS);
            this.recommender = getRecommender();
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

    private ExecutorService getExecutor() {
        if (this.executor == null) {
            this.executor = Executors.newSingleThreadExecutor();
        }
        return this.executor;
    }

    @Override
    public void execute(Tuple tuple) {
        registerPreference((Preference) tuple.getValueByField(PREFERENCE));
        getOutputCollector().ack(tuple);
        if (nextRun == 0) setNextRun();
        if (!isRecommenderRunning && bufferQueue.size() > 1000 && nextRun < System.currentTimeMillis()) {
            try {
                isRecommenderRunning = true;
                LOG.info("Running recommendation...\t");
                new RecommendationRunner(drainBuffer()).run();
                //getExecutor().execute(new RecommendationRunner(drainBuffer()));
            } finally {
                setNextRun();
                isRecommenderRunning = false;
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
            trainingDataModel.setPreference(preference);
        } catch (InterruptedException e) {
            LOG.warn(e);
        }
    }

    private void setNextRun() {
        nextRun = System.currentTimeMillis() + ITERATION_LENGTH;
        LOG.info("Next recommendation on CU" + getTopologyContext().getThisTaskIndex() + " run at:" + new Date(nextRun));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DEFAULT_STREAM, new Fields(ESTIMATION));
        outputFieldsDeclarer.declareStream(QE_STREAM, new Fields(BOLT_IDX, PREFERENCE, ESTIMATION));
    }

    private class RecommendationRunner implements Runnable {
        private final Collection<Preference> batch;

        public RecommendationRunner(Collection<Preference> batch) {
            this.batch = batch;
        }

        @Override
        public void run() {
            try {
                getRecommender().refresh(new ArrayList<Refreshable>());
                for (Preference eachPreference : batch) {
                    try {
                        List<RecommendedItem> results = getRecommender().recommend(eachPreference.getUserID(), 10);
                        for (RecommendedItem eachItem : results) {
                            ResultPreference result = new ResultPreference(eachPreference.getUserID(), eachItem.getItemID(), eachItem.getValue());
                            getOutputCollector().emit(DEFAULT_STREAM, new Values(result));
                        }
                        getOutputCollector().emit(QE_STREAM, new Values(
                                getTopologyContext().getThisTaskIndex(),
                                eachPreference,
                                new ResultPreference(
                                        eachPreference.getUserID()
                                        , eachPreference.getItemID(),
                                        getRecommender().estimatePreference(eachPreference.getUserID(), eachPreference.getItemID()))));
                    } catch (NoSuchUserException | NoSuchItemException e) {
                        LOG.debug("No recommendation for " + eachPreference);
                    }
                }
            } catch (TasteException e) {
                LOG.warn(e);
            }
        }
    }
}
