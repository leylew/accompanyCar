package cn.edu.fudan.dsm.MultiIndexes.common.entity.rowkey;

import cn.edu.fudan.dsm.MultiIndexes.Utils.StringUtils;

public class TimeSeriesRowKey {

    public static int ROWKEY_FIXED_WIDTH = 13;

    private long first;

    public TimeSeriesRowKey(long first) {
        this.first = first;
    }

    @Override
    public String toString() {
        return StringUtils.toStringFixedWidth(first, ROWKEY_FIXED_WIDTH);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesRowKey that = (TimeSeriesRowKey) o;

        return first == that.first;
    }

    @Override
    public int hashCode() {
        return (int) (first ^ (first >>> 32));
    }

    public long getFirst() {
        return first;
    }
}