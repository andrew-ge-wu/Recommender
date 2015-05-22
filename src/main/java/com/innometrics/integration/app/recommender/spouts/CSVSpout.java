package com.innometrics.integration.app.recommender.spouts;

import au.com.bytecode.opencsv.CSVReader;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.innometrics.integration.app.recommender.ml.model.TrackedPreference;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;

import java.io.FileReader;
import java.util.Date;
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
    private long nrMessages = 0;

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
        } finally {
            System.out.println("SPOUT Started at:" + new Date());
        }
    }

    @Override
    public void nextTuple() {
        try {
            String[] line = reader.readNext();
            if (line != null && line.length == 4) {
                long id = linesRead.incrementAndGet();
                //Time.sleep(RandomUtils.nextInt(5));
                TrackedPreference preference = new TrackedPreference(new GenericPreference(Long.parseLong(line[0]), Long.parseLong(line[1]), Float.parseFloat(line[2])));
                _collector.emit(new Values(preference), new TrainingMessageID<>(id));
                nrMessages++;
            } else if (!isFinished) {
                System.out.println("Finished reading file, " + linesRead.get() + " lines read");
                isFinished = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}