package cn.edu.fudan.dsm.MultiIndexes.operator.file;

import cn.edu.fudan.dsm.MultiIndexes.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.MultiIndexes.common.entity.rowkey.TimeSeriesRowKey;
import cn.edu.fudan.dsm.MultiIndexes.operator.TimeSeriesOperator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TimeSeriesFileOperator implements TimeSeriesOperator {

    private int fileLength;
    private int seriesLength;
    private String path;
    private FileHandler fileHandler;

    public TimeSeriesFileOperator(String filepath, int fileLength, int seriesLength, boolean rebuild) throws IOException {
        this.path = filepath;
        this.fileLength = fileLength;
        this.seriesLength = seriesLength;
        fileHandler = new FileHandler(path, rebuild ? "w" : "r");
    }

    @Override
    public List<Double> readTimeSeries(int left, int length) throws IOException {
        List<Double> ret = new ArrayList<>();
        byte[] byteData = fileHandler.read(left * 8, length * 8);
        for(int  i = 0; i < length; i ++) {
            byte[] tb = new byte[8];
            System.arraycopy(byteData,i*8, tb,0,8);
            ret.add(bytes2Double(tb));
        }
        return ret;
    }

    @Override
    public TimeSeriesNodeIterator readAllTimeSeries() throws IOException {
        return null;
    }

    @Override
    public void writeTimeSeriesNode(TimeSeriesRowKey rowKey, TimeSeriesNode node) throws IOException {
        fileHandler.write(node.toBytes());
    }

    @Override
    public List<List<Double>> readAllTimeSeries(int length, int totalLength) throws IOException {
        List<List<Double>> getData = new ArrayList<>();
        byte[] byteData = new byte[1024 * 8];
        int pos = 0;
        int tmpListC = 0;
        List<Double> tmpList = new ArrayList<>();
        while(pos < totalLength){
            byteData = fileHandler.read(pos * 8, Math.min((totalLength - pos) * 8, byteData.length));
            for(int i = 0;i < Math.min(totalLength - pos, byteData.length / 8);i ++){
                byte[] tb = new byte[8];
                System.arraycopy(byteData,i*8,tb,0,8);
                tmpList.add(bytes2Double(tb));
                tmpListC ++;
                if(tmpListC == length){
                    List<Double> addList = new ArrayList<>();
                    addList.addAll(tmpList);
                    getData.add(addList);
                    tmpList.clear();
                    tmpListC = 0;
                }
            }
            pos += 1024;
        }
        if(tmpListC != 0)getData.add(tmpList);

        return getData;
    }

    @Override
    public Double TsDist(List<Double> ts, List<Double> queryData) {
        Double dist = 0.0;
        for(int i = 0; i < ts.size(); i++)dist += (ts.get(i) - queryData.get(i)) * (ts.get(i) - queryData.get(i));
        return Math.sqrt(dist);
    }

    @Override
    public Double TsNormED(List<Double> tss, List<Double> queryData, double alpha, double beta) {
        Double dist = 0.0;
        int tsLen = queryData.size();
        double ext = 0.0;
        double ex2t = 0.0;
        double exq = 0.0;
        double ex2q = 0.0;
        for(int i = 0; i < tsLen; i ++){
            ext += tss.get(i);
            exq += queryData.get(i);
            ex2t += tss.get(i) * tss.get(i);
            ex2q += queryData.get(i) * queryData.get(i);
        }
        ext /= tsLen;
        ex2t = Math.sqrt(ex2t / tsLen - ext * ext);

        exq /= tsLen;
        ex2q = Math.sqrt(ex2q / tsLen - exq * exq);

        if((ex2t / ex2q) > alpha || (ex2t / ex2q) < 1/alpha) return 1000000.0;
        if((ext - exq) > beta || (ext - exq) < -beta) return 1000000.0;

        for(int i =0; i < tsLen; i++){
            dist += (((tss.get(i) - ext) / ex2t) - ((queryData.get(i) - exq) / ex2q)) * (((tss.get(i) - ext) / ex2t) - ((queryData.get(i) - exq) / ex2q));
        }
        //System.out.println(Math.sqrt(dist));
        return Math.sqrt(dist);
    }

    @Override
    public void close() throws IOException {
        fileHandler.close();
    }
    public double bytes2Double(byte[] arr) {
        long value = 0;
        for (int i = 7; i >= 0; i--) {
            value |= ((long) (arr[i] & 0xff)) << (8 * (7 -  i));
        }
        return Double.longBitsToDouble(value);
    }
}