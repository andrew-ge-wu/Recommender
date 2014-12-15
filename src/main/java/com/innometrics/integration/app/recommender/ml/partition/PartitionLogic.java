package com.innometrics.integration.app.recommender.ml.partition;

/**
 * @author andrew, Innometrics
 */
public interface PartitionLogic {
    String getPartitionString(String userId, String itemId, long timestamp);
}
