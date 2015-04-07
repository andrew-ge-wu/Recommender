package com.innometrics.integration.app.recommender.ml.model;

import java.util.Collection;
import java.util.HashSet;

/**
 * @author andrew, Innometrics
 */
public class Batch<T> {
    private final long executionTime;
    private final int maxSize;
    private final Collection<T> storage;

    public Batch(long executionTime, int maxSize) {
        this.storage = new HashSet<>();
        this.executionTime = executionTime;
        this.maxSize = maxSize;
    }

    public boolean shouldBeExecuted() {
        return System.currentTimeMillis() > executionTime || storage.size() >= maxSize;
    }

    public void add(T obj) {
        storage.add(obj);
    }

    public Collection<T> getStorage() {
        return storage;
    }
}
