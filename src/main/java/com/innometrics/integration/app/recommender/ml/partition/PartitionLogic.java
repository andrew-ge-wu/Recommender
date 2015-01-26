package com.innometrics.integration.app.recommender.ml.partition;

import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public interface PartitionLogic {
    String[] groupingFields();

    String[] getPartitionStrings(long userId, long itemId, long timestamp);

    String[] getPartitionStrings(long userID, long itemID);

    String[] getPartitionStrings(Preference preference);
}
