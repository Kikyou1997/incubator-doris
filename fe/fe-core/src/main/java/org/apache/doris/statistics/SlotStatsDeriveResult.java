package org.apache.doris.statistics;

public class SlotStatsDeriveResult {

    // number of distinct value
    private long ndv;
    private long max;
    private long min;

    public long getNdv() {
        return ndv;
    }

    public void setNdv(long ndv) {
        this.ndv = ndv;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }
}
