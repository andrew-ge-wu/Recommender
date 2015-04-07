package com.innometrics.integration.app.recommender.ml.partition;

import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public interface PartitionLogic {
    void setMaxPartition(int maxIdx);

    String[] groupingFields();

    String[] getPartitionStrings(long userId, long itemId, long timestamp, int idx);

    String[] getPartitionStrings(long userID, long itemID, int idx);

    String[] getPartitionStrings(Preference preference, int idx);
}
