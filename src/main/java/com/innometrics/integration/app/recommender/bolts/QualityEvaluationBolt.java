package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.tuple.Tuple;
import com.innometrics.integration.app.recommender.ml.model.ResultStats;
import com.innometrics.integration.app.recommender.utils.MAEUtil;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.model.Preference;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class QualityEvaluationBolt extends AbstractRichBolt {
    private static final Logger LOG = Logger.getLogger(PartitionBolt.class);
    private Map<Integer, ResultStats> results = new ConcurrentHashMap<Integer, ResultStats>();

    @Override
    public void execute(Tuple tuple) {
        getOutputCollector().ack(tuple);
        int idx = tuple.getIntegerByField(BOLT_IDX);
        Preference feedback = (Preference) tuple.getValueByField(PREFERENCE);
        Preference estimation = (Preference) tuple.getValueByField(ESTIMATION);
        registerError(idx, estimation, feedback);
        ResultStats result = results.get(idx);
        if (result.getNrSamples() % 100 == 0) {
            LOG.info("Current MAE: CU index(" + idx + ") NrSample(" + result.getNrSamples() + ") MAE(" + result.getAvgError() + ") MAX(" + result.getHighestRating() + ") " + result.getAvgError() * 100 / result.getHighestRating() + "%");
        }
    }


    private synchronized void registerError(int index, Preference estimation, Preference feedback) {
        ResultStats currentResult = results.get(index);
        results.put(index, MAEUtil.updateMAE(currentResult, feedback, estimation));
    }
}
