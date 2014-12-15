package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author andrew, Innometrics
 */
public class ResultWritingBolt extends BaseRichBolt
{
    private OutputCollector outputCollector;
    private TopologyContext topologyContext;
    private Map config;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.topologyContext = topologyContext;
        this.config = map;
    }

    @Override
    public void execute(Tuple tuple) {
     //   System.out.println(topologyContext.getThisComponentId() + "-" + topologyContext.getThisTaskIndex() + " received:" + tuple.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //TODO:To be fixed
    }
}
