package com.innometrics.integration.app.recommender.repository.impl;

import com.innometrics.integration.app.recommender.repository.TrainingDataModel;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * @author andrew, Innometrics
 */
public class MemoryTrainingDataModel implements TrainingDataModel {
    FastByIDMap<PreferenceArray> data;
    long updateTimestamp;

    public MemoryTrainingDataModel(int maxSize) {
        data = new FastByIDMap<>(0, maxSize);
    }

    @Override
    public DataModel getDataModel() {
        return new GenericDataModel(data);
    }

    @Override
    public void setPreference(Preference preference) {
        PreferenceArray currentItems = data.get(preference.getUserID());
    }

    @Override
    public Preference getPreference(long userId, long itemId) {
        return null;  //TODO:To be fixed
    }
}
