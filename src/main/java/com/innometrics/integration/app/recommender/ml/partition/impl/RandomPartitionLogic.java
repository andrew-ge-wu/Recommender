package com.innometrics.integration.app.recommender.ml.partition.impl;

import com.innometrics.integration.app.recommender.ml.partition.PartitionLogic;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public class RandomPartitionLogic implements PartitionLogic {
    @Override
    public String getPartitionString(long userID, long itemId, long timestamp) {
        return RandomStringUtils.randomAlphanumeric(12);
    }

    @Override
    public String getPartitionString(long userID, long itemID) {
        return getPartitionString(userID, itemID, System.currentTimeMillis());
    }

    @Override
    public String getPartitionString(Preference preference) {
        return getPartitionString(preference.getUserID(), preference.getItemID());
    }
}
