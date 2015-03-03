package com.innometrics.integration.app.recommender.spouts;

/**
 * @author andrew, Innometrics
 */
public class TrainingMessageID<T> {
    private final long createTime;
    private final T id;

    public TrainingMessageID(T id) {
        createTime = System.currentTimeMillis();
        this.id=id;
    }

    public long getCreateTime() {
        return createTime;
    }

    public T getId() {
        return id;
    }
}
