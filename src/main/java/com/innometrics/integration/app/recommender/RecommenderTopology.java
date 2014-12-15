package com.innometrics.integration.app.recommender;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.innometrics.integration.app.recommender.bolts.CalculationUnit;
import com.innometrics.integration.app.recommender.bolts.PartitionBolt;
import com.innometrics.integration.app.recommender.bolts.ResultWritingBolt;
import com.innometrics.integration.app.recommender.spouts.CSVSpout;
import com.innometrics.integration.app.recommender.utils.Constants;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import static com.innometrics.integration.app.recommender.utils.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class RecommenderTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TOPOLOGY_TRAINING_SOURCE, new CSVSpout("ratings.csv", ',', true)).setNumTasks(1);
        builder.setBolt(TOPOLOGY_TRAINING_PARTITION, new PartitionBolt()).shuffleGrouping(TOPOLOGY_TRAINING_SOURCE).setNumTasks(2);
        builder.setBolt(TOPOLOGY_TRAINING_CALC, new CalculationUnit()).fieldsGrouping(TOPOLOGY_TRAINING_PARTITION, new Fields(PARTITION_ID)).setNumTasks(4);
        builder.setBolt(TOPOLOGY_TRAINING_WRITING, new ResultWritingBolt()).shuffleGrouping(TOPOLOGY_TRAINING_CALC).setNumTasks(6);

        new LocalCluster().submitTopology("Recommender", new Config(), builder.createTopology());
    }

    PropertiesConfiguration getConfiguration() throws ConfigurationException {
        return new PropertiesConfiguration(ClassLoader.getSystemResource(Constants.CONFIG_RESOURCE));
    }
}
