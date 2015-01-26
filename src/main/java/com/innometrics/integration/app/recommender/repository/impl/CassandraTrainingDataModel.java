package com.innometrics.integration.app.recommender.repository.impl;

import com.innometrics.integration.app.recommender.utils.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.mahout.cf.taste.impl.model.cassandra.CassandraDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;

import java.util.concurrent.TimeUnit;

/**
 * @author andrew, Innometrics
 */
public class CassandraTrainingDataModel extends AbstractCachedTrainingDataModel {
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String KEY_SPACE = "keyspace";

    private final Configuration configuration;
    private final DataModel dataModel;
    private final String id;

    public CassandraTrainingDataModel(String id, int cacheSize, long cacheTime, TimeUnit unit) throws ConfigurationException {
        super(cacheSize, cacheTime, unit);
        this.configuration = new Configuration(CassandraTrainingDataModel.class);
        this.dataModel = new CassandraDataModel(configuration.getString(HOST), configuration.getInt(PORT), configuration.getString(KEY_SPACE));
        this.id=id;
        init();
    }

    private void init() {

    }

    @Override
    public DataModel getDataModel() {
        return dataModel;
    }

    @Override
    protected void savePreference(Preference preference) {
        //TODO:To be fixed
    }
}
