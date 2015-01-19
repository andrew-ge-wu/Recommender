package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.innometrics.integration.app.recommender.repository.TrainingDataModel;
import com.innometrics.integration.app.recommender.repository.impl.MysqlTrainingDataModel;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.Preference;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static com.innometrics.integration.app.recommender.utils.Constants.PARTITION_ID;
import static com.innometrics.integration.app.recommender.utils.Constants.PREFERENCE;

/**
 * @author andrew, Innometrics
 */
public class TrainingBolt extends AbstractRichBolt {
    private TrainingDataModel trainingDataModel;

    @Override
    public void init() {
        try {
            this.trainingDataModel = new MysqlTrainingDataModel(getTopologyContext().getThisComponentId() + getTopologyContext().getThisTaskIndex(), 10000, 60, TimeUnit.SECONDS);
        } catch (IOException | ClassNotFoundException | SQLException | TasteException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        trainingDataModel.setPreference((Preference) tuple.getValueByField(PREFERENCE));
        getOutputCollector().emit(tuple.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(PARTITION_ID, PREFERENCE));

    }

}
