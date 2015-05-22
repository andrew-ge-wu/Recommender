package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.tuple.Tuple;
import com.innometrics.integration.app.recommender.ml.model.MultiSampleResult;
import com.innometrics.integration.app.recommender.ml.model.ResultPreference;
import com.innometrics.integration.app.recommender.ml.model.ResultStats;
import com.innometrics.integration.app.recommender.utils.Constants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.model.Preference;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.innometrics.integration.app.recommender.utils.Constants.ESTIMATION;
import static com.innometrics.integration.app.recommender.utils.Constants.PREFERENCE;

/**
 * @author andrew, Innometrics
 */
public class QualityEvaluationBolt extends AbstractRichBolt {
    private static final Logger LOG = Logger.getLogger(PartitionBolt.class);
    private ResultStats result;
    private Map<Pair<Long, Long>, MultiSampleResult> tempStorage = new ConcurrentHashMap<>();
    private volatile long totalTimeForEstimation = 0;

    @Override
    public void execute(Tuple tuple) {
        ack(tuple);
        Preference feedback = (Preference) tuple.getValueByField(PREFERENCE);
        ResultPreference estimation = (ResultPreference) tuple.getValueByField(ESTIMATION);
        if (result == null) {
            result = new ResultStats(feedback.getValue());
        } else {
            result.setHighestRatting(feedback.getValue());
        }
        totalTimeForEstimation += estimation.getTimeTook();
        registerError(estimation, feedback);
    }


    private synchronized void registerError(Preference estimation, Preference feedback) {
        Pair<Long, Long> key = MultiSampleResult.getKey(feedback);
        if (!tempStorage.containsKey(key)) {
            tempStorage.put(key, new MultiSampleResult(feedback));
        }
        MultiSampleResult toOperate = tempStorage.get(key);
        toOperate.addSample(estimation);
        if (toOperate.getEstimations().size() == Constants.NR_CROSS_REF) {
            float realValue = toOperate.getRealValue();
            float estimateValue = avg(toOperate.getEstimations());
            tempStorage.remove(key);
            result.setSample(Math.abs(realValue - estimateValue));
            if (result.getNrSamples() > 0 && result.getNrSamples() % 1000 == 0) {
                LOG.info("Current MAE: NrSample(" + result.getNrSamples() + ") MAE(" + result.getAvgError() + ") MAX(" + result.getHighestRating() + ") " + result.getAvgError() * 100 / result.getHighestRating() + "% Average time to serving:" + totalTimeForEstimation / result.getNrSamples());
            }
        }
    }

    private float avg(Collection<Float> toCalc) {
        float sum = 0;
        for (float eachFloat : toCalc) {
            sum += eachFloat;
        }
        return sum / toCalc.size();
    }
}
