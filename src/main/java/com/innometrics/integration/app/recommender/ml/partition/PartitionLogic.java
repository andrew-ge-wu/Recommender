package com.innometrics.integration.app.recommender.ml.partition;

import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public interface PartitionLogic {
    String getPartitionString(long userId, long itemId, long timestamp);

    String getPartitionString(long userID, long itemID);

    String getPartitionString(Preference preference);
}
