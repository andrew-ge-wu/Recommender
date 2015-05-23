package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import com.innometrics.integration.app.recommender.ml.model.ResultPreference;
import com.innometrics.integration.app.recommender.ml.model.TrackedPreference;
import com.innometrics.integration.app.recommender.ml.model.UniqueQueue;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.util.*;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class CalculationBolt extends AbstractRichBolt {
    private static final Logger LOG = Logger.getLogger(CalculationBolt.class);
    private UniqueQueue<Long> removingQueue = new UniqueQueue<>();
    private final FastByIDMap<PreferenceArray> storage = new FastByIDMap<>();
    private int totalItems = 0;

    @Override
    public void init() {
    }

    private Recommender getRecommender() throws TasteException {
        GenericDataModel model = new GenericDataModel(storage);
        return new SVDRecommender(model, new ALSWRFactorizer(model, 10, 0.01, 5));
    }


    @Override
    public void execute(Tuple tuple) {
        Collection<TrackedPreference> preferenceCollection = (Collection<TrackedPreference>) tuple.getValueByField(PREFERENCE);
        if (preferenceCollection != null) {
            try {
                for (TrackedPreference trackedPreference : preferenceCollection) {
                    Preference preference = trackedPreference.getPreference();
                    if (!storage.containsKey(preference.getUserID())) {
                        storage.put(preference.getUserID(), new GenericUserPreferenceArray(Lists.newArrayList(preference)));
                    } else {
                        List<Preference> newList = new ArrayList<>();
                        newList.add(preference);
                        for (Preference earlierPreference : storage.get(preference.getUserID())) {
                            newList.add(earlierPreference);
                        }
                        storage.put(preference.getUserID(), new GenericUserPreferenceArray(newList));
                    }
                    totalItems++;
                    removingQueue.add(preference.getUserID());
                }
                while (totalItems > CALCULATION_SIZE) {
                    long toRemove = removingQueue.poll();
                    PreferenceArray removed = storage.remove(toRemove);
                    totalItems -= removed.length();
                    LOG.debug("Reducing items current:" + totalItems + " max:" + BATCH_LIMIT * 10 + " removing:" + removed.length());
                }
                LOG.info("Running calculation on " + totalItems + " items " + storage.size() + " users.");
                Recommender recommender = getRecommender();
                Set<Long> uids = new HashSet<>();
                for (TrackedPreference trackedPreference : preferenceCollection) {
                    Preference eachPreference = trackedPreference.getPreference();
                    try {
                        long uid = eachPreference.getUserID();
                        if (!uids.contains(uid)) {
                            List<RecommendedItem> results = recommender.recommend(eachPreference.getUserID(), 10);
                            for (RecommendedItem eachItem : results) {
                                emit(DEFAULT_STREAM, new Values(new ResultPreference(uid, eachItem.getItemID(), eachItem.getValue(), System.currentTimeMillis() - trackedPreference.getCreateTime())));
                                //defaultQueue.put(new ResultPreference(uid, eachItem.getItemID(), eachItem.getValue()));
                            }
                            uids.add(uid);
                        }
                        emit(QE_STREAM, tuple, new Values(getTopologyContext().getThisTaskIndex(), eachPreference, new ResultPreference(
                                eachPreference.getUserID()
                                , eachPreference.getItemID()
                                , recommender.estimatePreference(eachPreference.getUserID(), eachPreference.getItemID())
                                , System.currentTimeMillis() - trackedPreference.getCreateTime())));
                        ack(tuple);
                    } catch (NoSuchUserException | NoSuchItemException e) {
                        LOG.info("No recommendation for " + eachPreference);
                    }
                }
            } catch (TasteException e) {
                LOG.warn(e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DEFAULT_STREAM, new Fields(ESTIMATION));
        outputFieldsDeclarer.declareStream(QE_STREAM, new Fields(BOLT_IDX, PREFERENCE, ESTIMATION));
    }
}
