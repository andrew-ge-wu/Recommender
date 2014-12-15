package com.innometrics.integration.app.recommender.repository;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author andrew, Innometrics
 */
public class CSVTrainingDataModel implements TrainingDataModel {
    private final File dataFile;
    private FileDataModel dataModel;

    public CSVTrainingDataModel(File file) throws IOException {
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
    public synchronized void setPreference(long userId, long itemId, float preference, long timestamp) {
        CSVWriter writer = null;
        try {
            writer = new CSVWriter(new FileWriter(dataFile, true), ',', '\0');
            writer.writeNext(String.valueOf(userId), String.valueOf(itemId), String.valueOf(preference), String.valueOf(timestamp));
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
