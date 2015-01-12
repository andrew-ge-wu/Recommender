package com.innometrics.integration.app.recommender.repository.impl;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author andrew, Innometrics
 */
public class CSVTrainingDataModel extends AbstractCachedTrainingDataModel {
    private final File dataFile;
    private FileDataModel dataModel;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public CSVTrainingDataModel(File file) throws IOException {
        super(5000, 1, TimeUnit.DAYS);
        this.dataFile = file;
        if (!this.dataFile.exists()) {
            this.dataFile.createNewFile();
        }
    }

    @Override
    public DataModel getDataModel() {
        if (dataModel == null) {
            try {
                dataModel = new FileDataModel(this.dataFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return dataModel;
    }

    @Override
    protected void savePreference(final Preference preference) {
        executorService.execute(new CSVPreferenceSaver(preference));
    }

    private class CSVPreferenceSaver implements Runnable {

        private final Preference preference;

        public CSVPreferenceSaver(Preference preference) {
            this.preference = preference;
        }

        @Override
        public void run() {

            CSVWriter writer = null;
            try {
                writer = new CSVWriter(new FileWriter(dataFile, true), ',', '\0');
                writer.writeNext(String.valueOf(preference.getUserID()), String.valueOf(preference.getItemID()), String.valueOf(preference.getValue()));
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
