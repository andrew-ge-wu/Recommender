package com.innometrics.integration.app.recommender.spouts;

import au.com.bytecode.opencsv.CSVReader;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author andrew, Innometrics
 */
public class CSVSpout extends AbstractLearningSpout {
    private final String fileName;
    private final char separator;
    private boolean includesHeaderRow;
    private SpoutOutputCollector _collector;
    private CSVReader reader;
    private AtomicLong linesRead;
    private boolean isFinished = false;

    public CSVSpout(String filename, char separator, boolean includesHeaderRow) {
        this.fileName = filename;
        this.separator = separator;
        this.includesHeaderRow = includesHeaderRow;
        linesRead = new AtomicLong(0);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        try {
            reader = new CSVReader(new FileReader(fileName), separator);
            // read and ignore the header if one exists
            if (includesHeaderRow) reader.readNext();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String[] line = reader.readNext();
            if (line != null && line.length == 4) {
                long id = linesRead.incrementAndGet();
                _collector.emit(new Values(line), id);
            } else if (!isFinished) {
                System.out.println("Finished reading file, " + linesRead.get() + " lines read");
                isFinished=true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
        System.err.println("Failed tuple with id " + id);
    }

}