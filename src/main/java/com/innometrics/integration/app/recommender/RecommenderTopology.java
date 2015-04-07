package com.innometrics.integration.app.recommender;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.innometrics.integration.app.recommender.bolts.CalculationBolt;
import com.innometrics.integration.app.recommender.bolts.PartitionBolt;
import com.innometrics.integration.app.recommender.bolts.QualityEvaluationBolt;
import com.innometrics.integration.app.recommender.bolts.ResultWritingBolt;
import com.innometrics.integration.app.recommender.ml.model.ResultPreference;
import com.innometrics.integration.app.recommender.spouts.CSVSpout;
import com.innometrics.integration.app.recommender.utils.Constants;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class RecommenderTopology {
    public static final int NR_CU = 7;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TOPOLOGY_TRAINING_SOURCE, new CSVSpout("ratings.csv", ',', true)).setNumTasks(1);
        PartitionBolt partitionBolt = new PartitionBolt();
        builder.setBolt(TOPOLOGY_TRAINING_PARTITION, partitionBolt).shuffleGrouping(TOPOLOGY_TRAINING_SOURCE).setNumTasks(1);
        builder.setBolt(TOPOLOGY_TRAINING_CALC, new CalculationBolt()).fieldsGrouping(TOPOLOGY_TRAINING_PARTITION, DEFAULT_STREAM, new Fields(partitionBolt.groupingFields())).setNumTasks(NR_CU);
        builder.setBolt(TOPOLOGY_TRAINING_WRITING, new ResultWritingBolt()).shuffleGrouping(TOPOLOGY_TRAINING_CALC, DEFAULT_STREAM).setNumTasks(NR_CU);
        builder.setBolt(TOPOLOGY_TRAINING_QE, new QualityEvaluationBolt()).fieldsGrouping(TOPOLOGY_TRAINING_CALC, QE_STREAM, new Fields(BOLT_IDX)).setNumTasks(1);
        new LocalCluster().submitTopology("Recommender", getConfig(), builder.createTopology());
    }


    private static Config getConfig() {
        Config toReturn = new Config();
        toReturn.registerSerialization(GenericPreference.class);
        toReturn.registerSerialization(ResultPreference.class);
        toReturn.setMessageTimeoutSecs(600);
        toReturn.setMaxSpoutPending(Constants.BATCH_LIMIT * NR_CU);
        toReturn.setNumAckers(NR_CU);
        return toReturn;
    }
}
