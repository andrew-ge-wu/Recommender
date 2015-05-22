package com.innometrics.integration.app.recommender.ml.model;

import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public class ResultPreference implements Preference {
    private final long userId;
    private final long itemId;
    private final float value;
    private final long timeTook;

    public ResultPreference(long userId, long itemId, float value, long timeTook) {
        this.userId = userId;
        this.itemId = itemId;
        this.value = value;
        this.timeTook = timeTook;
    }

    @Override
    public long getUserID() {
        return userId;
    }

    @Override
    public long getItemID() {
        return itemId;
    }

    @Override
    public float getValue() {
        return value;
    }

    @Override
    public void setValue(float value) {
        throw new UnsupportedOperationException("Can not modify immutable object");
    }

    public long getTimeTook() {
        return timeTook;
    }
}
