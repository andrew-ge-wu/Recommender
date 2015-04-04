package com.innometrics.integration.app.recommender.ml.model;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.mahout.cf.taste.model.Preference;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author andrew, Innometrics
 */
public class MultiSampleResult {
    private final ImmutablePair<Long, Long> key;
    private final float realValue;
    private final Collection<Float> estimations = new ArrayList<>();


    public MultiSampleResult(Preference preference) {
        this.key = getKey(preference);
        this.realValue = preference.getValue();
    }

    public static ImmutablePair<Long, Long> getKey(Preference preference) {
        return new ImmutablePair<>(preference.getUserID(), preference.getItemID());
    }

    public void addSample(Preference estimation) {
        if (estimation.getUserID() == key.getLeft() && estimation.getItemID() == key.getRight()) {
            addSample(estimation.getValue());
        } else {
            throw new RuntimeException("Un-match estimation:" + estimation + " need " + key);
        }
    }

    public void addSample(float estimation) {
        estimations.add(estimation);
    }

    public ImmutablePair<Long, Long> getKey() {
        return key;
    }

    public float getRealValue() {
        return realValue;
    }

    public Collection<Float> getEstimations() {
        return estimations;
    }
}
