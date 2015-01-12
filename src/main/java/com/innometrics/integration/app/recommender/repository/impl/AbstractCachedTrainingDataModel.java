package com.innometrics.integration.app.recommender.repository.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.innometrics.integration.app.recommender.repository.TrainingDataModel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.model.Preference;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author andrew, Innometrics
 */
public abstract class AbstractCachedTrainingDataModel implements TrainingDataModel {
    private final Cache<Pair<Long, Long>, Preference> cache;

    public AbstractCachedTrainingDataModel(int cacheSize, long cacheTime, TimeUnit unit) {
        cache = CacheBuilder.newBuilder().expireAfterAccess(cacheTime, unit).maximumSize(cacheSize).build();
    }

    @Override
    public final void setPreference(Preference preference) {
        cache.put(new ImmutablePair<>(preference.getUserID(), preference.getItemID()), preference);
        savePreference(preference);
    }

    protected abstract void savePreference(Preference preference);

    @Override
    public final Preference getPreference(final long userId, final long itemId) {
        try {
            return cache.get(new ImmutablePair<>(userId, itemId), new Callable<Preference>() {
                @Override
                public Preference call() throws Exception {
                    try {
                        Float value = getDataModel().getPreferenceValue(userId, itemId);
                        return new GenericPreference(userId, itemId, value);
                    } catch (Exception ex) {
                        return new GenericPreference(userId, itemId, Float.MIN_VALUE);
                    }
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
