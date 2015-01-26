package com.innometrics.integration.app.recommender.ml.partition.impl;

import com.innometrics.integration.app.recommender.ml.partition.PartitionLogic;
import com.innometrics.integration.app.recommender.utils.Constants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public class RandomPartitionLogic implements PartitionLogic {
    @Override
    public String[] groupingFields() {
        return new String[]{Constants.PARTITION_ID};
    }

    @Override
    public String[] getPartitionStrings(long userID, long itemId, long timestamp) {
        return new String[]{RandomStringUtils.random(12)};
    }

    @Override
    public String[] getPartitionStrings(long userID, long itemID) {
        return getPartitionStrings(userID, itemID, System.currentTimeMillis());
    }

    @Override
    public String[] getPartitionStrings(Preference preference) {
        return getPartitionStrings(preference.getUserID(), preference.getItemID());
    }
}
