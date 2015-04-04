package com.innometrics.integration.app.recommender.ml.model;

/**
 * @author andrew, Innometrics
 */
public class ResultStats {
    private long nrSamples;
    private float totalError;
    private float highestRating;

    public ResultStats(float highestRating) {
        setHighestRatting(highestRating);
    }

    public ResultStats(long nrSamples, float totalError, float highestRating) {
        this.nrSamples = nrSamples;
        this.totalError = totalError;
        this.highestRating = highestRating;
    }

    public synchronized void setHighestRatting(float highestRating) {
        if (highestRating > this.highestRating) {
            this.highestRating = highestRating;
        }
    }

    public synchronized void setSample(float error) {
        this.totalError += error;
        this.nrSamples++;
    }

    public long getNrSamples() {
        return nrSamples;
    }

    public float getAvgError() {
        return totalError / nrSamples;
    }

    public float getHighestRating() {
        return highestRating;
    }
}
