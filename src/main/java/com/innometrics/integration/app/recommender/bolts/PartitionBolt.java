package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.ml.model.Batch;
import com.innometrics.integration.app.recommender.ml.model.TrackedPreference;
import com.innometrics.integration.app.recommender.ml.partition.PartitionLogic;
import com.innometrics.integration.app.recommender.ml.partition.impl.UserPartitionLogic;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class PartitionBolt extends AbstractRichBolt {
    private static final Logger LOG = Logger.getLogger(PartitionBolt.class);
    private transient PartitionLogic partitionLogic;
    Map<String, Batch<TrackedPreference>> storage;
    Map<String, String[]> keyMap = new HashMap<>();


    @Override
    protected void init() {
        storage = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        TrackedPreference preference = TrackedPreference.class.cast(tuple.getValueByField(PREFERENCE));
        ack(tuple);
        for (int i = 0; i < NR_CROSS_REF; i++) {
            String[] partition = getPartitionLogic().getPartitionStrings(preference.getPreference(), i);
            Object[] toSend = new Object[partition.length + 1];
            System.arraycopy(partition, 0, toSend, 0, partition.length);
            toSend[toSend.length - 1] = preference;
            String key = ArrayUtils.toString(partition);
            if (!storage.containsKey(key)) {
                storage.put(key, new Batch<TrackedPreference>(System.currentTimeMillis() + BATCH_DELAY, BATCH_LIMIT));
                keyMap.put(key, partition);
            }
            storage.get(key).add(preference);
        }
        for (Map.Entry<String, Batch<TrackedPreference>> entry : storage.entrySet()) {
            if (entry.getValue().shouldBeExecuted()) {
                String[] partition = keyMap.get(entry.getKey());
                LOG.debug("Sending batch:" + entry.getValue().getStorage().size());
                Object[] toSend = new Object[partition.length + 1];
                System.arraycopy(partition, 0, toSend, 0, partition.length);
                toSend[toSend.length - 1] = entry.getValue().getStorage();
                emit(DEFAULT_STREAM, new Values(toSend));
                storage.remove(entry.getKey());
            }
        }

    }

    private PartitionLogic getPartitionLogic() {
        if (partitionLogic == null) {
            partitionLogic = new UserPartitionLogic();
        }
        partitionLogic.setMaxPartition(NR_CU);
        return partitionLogic;
    }

    public String[] groupingFields() {
        return getPartitionLogic().groupingFields();
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DEFAULT_STREAM, new Fields((String[]) ArrayUtils.add(groupingFields(), PREFERENCE)));
    }
}
