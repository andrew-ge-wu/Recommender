package com.innometrics.integration.app.recommender.ml.partition.impl;

import com.innometrics.integration.app.recommender.ml.partition.PartitionLogic;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * @author andrew, Innometrics
 */
public class RandomPartitionLogic implements PartitionLogic {
    @Override
    public String getPartitionString(String userId, String itemId, long timestamp) {
        return RandomStringUtils.randomAlphanumeric(12);
    }
}
