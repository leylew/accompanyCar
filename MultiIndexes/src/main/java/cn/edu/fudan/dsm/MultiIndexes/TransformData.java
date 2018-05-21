package cn.edu.fudan.dsm.MultiIndexes;

import cn.edu.fudan.dsm.MultiIndexes.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.MultiIndexes.operator.file.TimeSeriesFileOperator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class TransformData {
    public static void main(String[] args){
        try {
            int fileLength = 1000000;
            int seriesLength = 100;

            File wfile = new File("files/data/1000000d");
            FileOutputStream fos = new FileOutputStream(wfile);
            TimeSeriesOperator timeSeriesOperator = new TimeSeriesFileOperator("files/data/1000000", fileLength, seriesLength, false);

            for(int i = 0; i < fileLength / seriesLength; i++){
                double ex = 0.0;
                double ex2 = 0.0;
                List<Double> ts = timeSeriesOperator.readTimeSeries(i * seriesLength, seriesLength);
                for(int j = 0; j < ts.size(); j++){
                    ex += ts.get(j);
                    ex2 += ts.get(j) * ts.get(j);
                }
                ex /= seriesLength;
                ex2 = Math.sqrt(ex2 / seriesLength - ex * ex);
                System.out.println(ex + " " + ex2);
                for(int j = 0; j < ts.size() - 1; j++){
                    String s = (ts.get(j) - ex) / ex2 + " ";
                    fos.write(s.getBytes());
                }
                fos.write(((ts.get(ts.size() - 1)- ex) / ex2 + "\n").getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
