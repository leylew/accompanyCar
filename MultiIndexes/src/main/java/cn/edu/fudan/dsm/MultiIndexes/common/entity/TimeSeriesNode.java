package cn.edu.fudan.dsm.MultiIndexes.common.entity;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class TimeSeriesNode {

    public static int ROW_LENGTH = 1000;

    private List<Double> data;

    public TimeSeriesNode() {
        this.data = new ArrayList<>();
    }

    public byte[] toBytes() {
        byte[] result = new byte[Bytes.SIZEOF_DOUBLE * data.size()];
        for (int i = 0; i < data.size(); i++) {
            System.arraycopy(Bytes.toBytes(data.get(i)), 0, result, Bytes.SIZEOF_DOUBLE * i, Bytes.SIZEOF_DOUBLE);
        }
        return result;
    }

    public void parseBytes(byte[] concatData) {
        for (int i = 0; i < concatData.length / Bytes.SIZEOF_DOUBLE; i++) {
            byte[] temp = new byte[Bytes.SIZEOF_DOUBLE];
            System.arraycopy(concatData, Bytes.SIZEOF_DOUBLE * i, temp, 0, Bytes.SIZEOF_DOUBLE);
            data.add(Bytes.toDouble(temp));
        }
    }

    @Override
    public String toString() {
        return "TimeSeriesNode{" + "data=" + data + '}';
    }

    public List<Double> getData() {
        return data;
    }

    public void setData(List<Double> data) {
        this.data = data;
    }
}
