package com.innometrics.integration.app.recommender.repository;

import org.apache.mahout.cf.taste.model.DataModel;

/**
 * @author andrew, Innometrics
 */
public interface TrainingDataModel {
    DataModel getDataModel();

    void setPreference(long userId, long itemId, float preference, long timestamp);
}
