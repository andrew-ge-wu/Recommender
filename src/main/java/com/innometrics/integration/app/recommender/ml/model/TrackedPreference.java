package com.innometrics.integration.app.recommender.ml.model;

import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public class TrackedPreference {
    private final Preference preference;
    private final long createTime;

    public TrackedPreference(Preference preference) {
        this.preference = preference;
        this.createTime = System.currentTimeMillis();
    }

    public Preference getPreference() {
        return preference;
    }

    public long getCreateTime() {
        return createTime;
    }
}
