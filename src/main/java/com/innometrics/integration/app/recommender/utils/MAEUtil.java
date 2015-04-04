package com.innometrics.integration.app.recommender.utils;

import com.innometrics.integration.app.recommender.ml.model.ResultStats;
import org.apache.mahout.cf.taste.model.Preference;

/**
 * @author andrew, Innometrics
 */
public class MAEUtil {

    public static ResultStats updateMAE(ResultStats currentResult, Preference feedback, Preference estimation) {
        float error = getError(estimation, feedback);
        currentResult.setHighestRatting(feedback.getValue());
        currentResult.setSample(error);
        return currentResult;
    }

    public static ResultStats updateMAE(ResultStats currentResult, float error) {
        if (currentResult == null) {
            currentResult = new ResultStats(error);
        }
        currentResult.setSample(error);
        return currentResult;
    }

    public static float getError(Preference estimation, Preference feedback) {
        return Math.abs(estimation.getValue() - feedback.getValue());
    }
}
