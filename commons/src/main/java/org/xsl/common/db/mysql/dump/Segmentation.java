package org.xsl.common.db.mysql.dump;

/**
 * Created by xiongsenlin on 15/9/27.
 */
public class Segmentation {
    private int retryTimes = 0;
    private String lowerBound;
    private String upperBound;

    public Segmentation() {}

    public Segmentation(String lower, String upper) {
        this.lowerBound = lower;
        this.upperBound = upper;
    }

    public String getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(String lowerBound) {
        this.lowerBound = lowerBound;
    }

    public String getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(String upperBound) {
        this.upperBound = upperBound;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }
}
