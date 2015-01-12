package com.innometrics.integration.app.recommender.repository;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public interface TrainingDataModel {
    DataModel getDataModel();

    void setPreference(Preference preference);

    Preference getPreference(long userId, long itemId);
}
