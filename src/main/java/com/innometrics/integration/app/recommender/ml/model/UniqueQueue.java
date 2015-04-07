package com.innometrics.integration.app.recommender.ml.model;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author andrew, Innometrics
 */
public class UniqueQueue<T> extends ConcurrentLinkedQueue<T> {


    @Override
    public boolean add(T t) {
        if (contains(t)) {
            remove(t);
        }
        return super.add(t);
    }

    @Override
    public boolean addAll(Collection<? extends T> arg0) {
        boolean ret = true;
        for (T t : arg0) {
            ret = add(t) && ret;
        }
        return ret;
    }
}