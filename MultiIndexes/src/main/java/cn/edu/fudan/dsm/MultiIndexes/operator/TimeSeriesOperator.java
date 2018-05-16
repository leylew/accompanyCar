package cn.edu.fudan.dsm.MultiIndexes.operator;

import cn.edu.fudan.dsm.MultiIndexes.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.MultiIndexes.common.entity.rowkey.TimeSeriesRowKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public interface TimeSeriesOperator extends Closeable {

    /**
     * Read time series in specific interval.
     *
     * @param left   the start time
     * @param length the length of time series
     * @return the time series data in the given time interval
     */
    List readTimeSeries(int left, int length) throws IOException;

    /**
     * Read time series in streaming fashion.
     *
     * @return the iterator used to read data
     */
    Iterator readAllTimeSeries() throws IOException;

    /**
     * Write a time series node into data (file/HBase table)
     *
     * @param rowKey row key of the time series node
     * @param node   time series node containing data
     */
    void writeTimeSeriesNode(TimeSeriesRowKey rowKey, TimeSeriesNode node) throws IOException;

    List readAllTimeSeries(int length, int totalLength) throws IOException;

    Double TsDist(List<Double> ts, List<Double> queryData);

    //Double TsNormED(List<Double> tss, List<Double> queryData);

    Double TsNormED(List<Double> tss, List<Double> queryData, double alpha, double beta);

}

