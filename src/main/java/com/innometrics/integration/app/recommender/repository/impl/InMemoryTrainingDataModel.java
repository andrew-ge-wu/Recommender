package com.innometrics.integration.app.recommender.repository.impl;

import com.innometrics.integration.app.recommender.repository.TrainingDataModel;
import com.innometrics.integration.app.recommender.repository.impl.datamodel.MutableDataModel;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;

import java.util.concurrent.LinkedTransferQueue;

/**
 * @author andrew, Innometrics
 */
public class InMemoryTrainingDataModel implements TrainingDataModel {
    private final MutableDataModel dataModel;
    private final LinkedTransferQueue<Preference> queue = new LinkedTransferQueue<>();

    public InMemoryTrainingDataModel(final int maxItem) {
        this.dataModel = new MutableDataModel();
        new Thread(new Runnable() {
            @Override
            public void run() {
                if (queue.size() > maxItem) {
                    while (queue.size() > maxItem) {
                        Preference preference = queue.poll();
                        dataModel.removePreference(preference.getUserID(), preference.getItemID());
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public DataModel getDataModel() {

        return dataModel;
    }

    @Override
    public void setPreference(Preference preference) {
        this.dataModel.setPreference(preference.getUserID(), preference.getItemID(), preference.getValue());
        queue.add(preference);
    }

    @Override
    public Preference getPreference(long userId, long itemId) {
        try {
            return new GenericPreference(userId, itemId, this.dataModel.getPreferenceValue(userId, itemId));
        } catch (TasteException e) {
            e.printStackTrace();
            return null;
        }
    }
}
