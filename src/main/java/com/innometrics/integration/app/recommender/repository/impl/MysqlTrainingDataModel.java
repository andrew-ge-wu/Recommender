package com.innometrics.integration.app.recommender.repository.impl;

import com.innometrics.integration.app.recommender.utils.Configuration;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.ReloadFromJDBCDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.innometrics.integration.app.recommender.utils.Constants.*;


/**
 * @author andrew, Innometrics
 */
public class MysqlTrainingDataModel extends AbstractCachedTrainingDataModel {
    private static final Logger logger = Logger.getLogger(MysqlTrainingDataModel.class.getCanonicalName());
    private final String id;
    private final ReloadFromJDBCDataModel dataModel;
    private final Configuration configuration;
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "database";
    private PreparedStatement insertStatement;

    public MysqlTrainingDataModel(String id, int cacheSize, long cacheTime, TimeUnit unit) throws ConfigurationException, TasteException, SQLException, IOException, ClassNotFoundException {
        super(cacheSize, cacheTime, unit);
        this.id = id;
        this.configuration = new Configuration(MysqlTrainingDataModel.class);
        DataSource dataSource = getDataSource();
        prepareDatabase(dataSource);
        dataModel = new ReloadFromJDBCDataModel(new MySQLJDBCDataModel(dataSource, getTableName(), TASTE_USER_ID_COLUMN, TASTE_ITEM_ID_COLUMN, TASTE_PREFERENCE_COLUMN, TASTE_TIMESTAMP_COLUMN));
    }

    private DataSource getDataSource() {
        MysqlDataSource toReturn = new MysqlDataSource();
        toReturn.setServerName(configuration.getString(HOST));
        toReturn.setPort(configuration.getInt(PORT));
        toReturn.setUser(configuration.getString(USERNAME));
        toReturn.setPassword(configuration.getString(PASSWORD));
        toReturn.setDatabaseName(configuration.getString(DATABASE));
        return toReturn;
    }

    private void prepareDatabase(DataSource dataSource) throws SQLException, ConfigurationException, IOException, ClassNotFoundException {
        dataSource.getConnection().createStatement().execute("CREATE TABLE IF NOT EXISTS `" + getTableName() + "` (" +
                "  `" + TASTE_USER_ID_COLUMN + "` BIGINT NOT NULL," +
                "  `" + TASTE_ITEM_ID_COLUMN + "` BIGINT NOT NULL," +
                "  `" + TASTE_PREFERENCE_COLUMN + "` FLOAT NOT NULL," +
                "  `" + TASTE_TIMESTAMP_COLUMN + "` BIGINT NULL," +
                "  PRIMARY KEY (`" + TASTE_USER_ID_COLUMN + "`, `" + TASTE_ITEM_ID_COLUMN + "`)," +
                "    KEY `" + TASTE_USER_ID_COLUMN + "_IDX` (`" + TASTE_USER_ID_COLUMN + "`)," +
                "    KEY `" + TASTE_ITEM_ID_COLUMN + "_IDX` (`" + TASTE_ITEM_ID_COLUMN + "`) )" +
                "  ENGINE=InnoDB DEFAULT CHARSET=utf8;");
        this.insertStatement = dataSource.getConnection().prepareStatement("INSERT INTO `" + getTableName() + "` " +
                "(`" + TASTE_USER_ID_COLUMN
                + "`,`" + TASTE_ITEM_ID_COLUMN
                + "`,`" + TASTE_PREFERENCE_COLUMN
                + "`,`" + TASTE_TIMESTAMP_COLUMN + "`) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE " +
                "`" + TASTE_PREFERENCE_COLUMN + "`=?," +
                "`" + TASTE_TIMESTAMP_COLUMN + "`=?");
    }

    private String getTableName() {
        return "training_data_" + this.id;
    }

    @Override
    protected void savePreference(Preference preference) {
        try {
            insertStatement.setLong(1, preference.getUserID());
            insertStatement.setLong(2, preference.getItemID());
            insertStatement.setFloat(3, preference.getValue());
            insertStatement.setLong(4, System.currentTimeMillis());
            insertStatement.setFloat(5, preference.getValue());
            insertStatement.setLong(6, System.currentTimeMillis());
            insertStatement.execute();
        } catch (SQLException e) {
            logger.warning("Request error:" + e);
        }
    }

    @Override
    public DataModel getDataModel() {
        return dataModel;
    }
}
