package cn.edu.fudan.dsm.MultiIndexes.common;

public class QuerySegment {

    private double mean;

    private double std;  // added for extended KV-match, which uses mean-std-combination as the rowkey of index

    private int order;

    private int count;

    private int Wu;

    private int indexBegin;
    private int indexEnd;

    public int getIndexBegin() {
        return indexBegin;
    }

    public void setIndexBegin(int indexBegin) {
        this.indexBegin = indexBegin;
    }

    public int getIndexEnd() {
        return indexEnd;
    }

    public void setIndexEnd(int indexEnd) {
        this.indexEnd = indexEnd;
    }

    public QuerySegment(double mean, int begin, int end, int order, int count, int Wu) {  // legacy for standard KV-match
        this.mean = mean;
        this.std = 0;
        this.order = order;
        this.count = count;
        this.Wu = Wu;
        this.indexBegin = begin;
        this.indexEnd = end;
    }

    public QuerySegment(double mean, double std, int order, int count, int Wu) {
        this.mean = mean;
        this.std = std;
        this.order = order;
        this.count = count;
        this.Wu = Wu;
    }

    @Override
    public String toString() {
        return String.valueOf(order) + "(" + String.valueOf(Wu) + ")";
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public double getStd() {
        return std;
    }

    public void setStd(double std) {
        this.std = std;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getWu() {
        return Wu;
    }

    public void setWu(int wu) {
        Wu = wu;
    }
}