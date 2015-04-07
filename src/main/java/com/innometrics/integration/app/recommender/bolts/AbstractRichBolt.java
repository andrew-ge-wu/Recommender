package com.innometrics.integration.app.recommender.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

/**
 * @author andrew, Innometrics
 */
public abstract class AbstractRichBolt extends BaseRichBolt {
    private volatile OutputCollector outputCollector;
    private TopologyContext topologyContext;
    private Map config;

    @Override
    public final void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.topologyContext = topologyContext;
        this.config = map;
        init();
    }

    protected void init() {
    }

    public TopologyContext getTopologyContext() {
        return topologyContext;
    }

    public Map getConfig() {
        return config;
    }

    private synchronized OutputCollector getOutputCollector() {
        return outputCollector;
    }

    public List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple) {
        return getOutputCollector().emit(streamId, anchor, tuple);
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {
        return getOutputCollector().emit(streamId, tuple);
    }

    public void ack(Tuple anchor) {
        getOutputCollector().ack(anchor);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
