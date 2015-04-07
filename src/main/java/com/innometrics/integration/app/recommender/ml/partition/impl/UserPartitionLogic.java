package com.innometrics.integration.app.recommender.ml.partition.impl;

import com.innometrics.integration.app.recommender.ml.partition.PartitionLogic;
import com.innometrics.integration.app.recommender.utils.Constants;
import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public class UserPartitionLogic implements PartitionLogic {
    private int maxPartition;

    @Override
    public void setMaxPartition(int maxIdx) {
        this.maxPartition = maxIdx;
    }

    @Override
    public String[] groupingFields() {
        return new String[]{Constants.PARTITION_ID, Constants.CROSS_REF_IDX};
    }

    @Override
    public String[] getPartitionStrings(long userID, long itemId, long timestamp, int idx) {
        return new String[]{String.valueOf(userID % maxPartition), String.valueOf(idx)};
    }

    @Override
    public String[] getPartitionStrings(long userID, long itemID, int idx) {
        return getPartitionStrings(userID, itemID, System.currentTimeMillis(), idx);
    }

    @Override
    public String[] getPartitionStrings(Preference preference, int idx) {
        return getPartitionStrings(preference.getUserID(), preference.getItemID(), idx);
    }
}
