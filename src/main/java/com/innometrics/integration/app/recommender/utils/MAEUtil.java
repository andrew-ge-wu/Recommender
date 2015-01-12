package com.innometrics.integration.app.recommender.utils;

import com.innometrics.integration.app.recommender.ml.model.ResultStats;
import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public class MAEUtil {

    public static ResultStats updateMAE(ResultStats currentResult, Preference feedback, Preference estimation) {
        float error = getError(estimation, feedback);
        if (currentResult == null) {
            return new ResultStats(1, error, feedback.getValue());
        } else {
            currentResult.setSample(error, feedback.getValue());
            return currentResult;
        }
    }

    public static float getError(Preference estimation, Preference feedback) {
        return Math.abs(estimation.getValue() - feedback.getValue());
    }
}
