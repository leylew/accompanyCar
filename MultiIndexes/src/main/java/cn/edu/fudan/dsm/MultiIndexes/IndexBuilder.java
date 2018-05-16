package cn.edu.fudan.dsm.MultiIndexes;

import cn.edu.fudan.dsm.MultiIndexes.Utils.MeanIntervalUtils;
import cn.edu.fudan.dsm.MultiIndexes.common.entity.IndexNode;
import cn.edu.fudan.dsm.MultiIndexes.operator.IndexOperator;
import cn.edu.fudan.dsm.MultiIndexes.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.MultiIndexes.operator.file.IndexFileOperator;
import cn.edu.fudan.dsm.MultiIndexes.operator.file.TimeSeriesFileOperator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class IndexBuilder {

    private static final int[] WuList = {25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 800};
    private static final boolean[] WuEnabled = {true, true, false, true, false, false, false, true, false, false, false, false, false, false, false, true, true};

    private TimeSeriesOperator timeSeriesOperator = null;
    private IndexOperator[] indexOperators = new IndexOperator[WuList.length];
    private int fileLength;
    private int seriesLength;

    public static void main(String args[]) throws IOException {
        System.out.print("Data Path = ");
        Scanner scanner = new Scanner(System.in);
        String path = scanner.nextLine();
        System.out.print("Data Length = ");
        int n1 = scanner.nextInt();

        System.out.print("Series Length = ");
        int n2 = scanner.nextInt();
        scanner.close();

        IndexBuilder indexBuilder = new IndexBuilder(path, n1, n2, "file");
        indexBuilder.buildIndexes();
    }

    public IndexBuilder(String filepath, int n1, int n2, String storageType) throws IOException {
        this.fileLength = n1;
        this.seriesLength = n2;
        switch (storageType) {
            case "file":
                timeSeriesOperator = new TimeSeriesFileOperator(filepath, fileLength, seriesLength, false);
                break;
            case "hdfs":
                break;
        }
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) continue;
            switch (storageType) {
                case "file":
                    indexOperators[i] = new IndexFileOperator("standard", WuList[i], true);
                    break;
                case "hdfs":
                    break;
            }
        }
    }

    private void buildIndexes() {

        // TODO: naive -> generate tables together
        try {
            List<List<Double>> listScanner = timeSeriesOperator.readAllTimeSeries(seriesLength, fileLength);
            for(int i = 0;  i < WuList.length && WuList[i] <= seriesLength; i++){
                if(!WuEnabled[i])continue;
                SingleIndexBuilder builder = new SingleIndexBuilder(WuList[i], listScanner, indexOperators[i]);
                builder.run();
            }
        } catch (IOException e) {
        }
    }

    private class SingleIndexBuilder {

        List<List<Double>> scanner;
        IndexOperator indexOperator;

        double[] t;  // data array and query array

        int w = -1;
        double[] buffer;

        // For every EPOCH points, all cumulative values, such as ex (sum), ex2 (sum square), will be restarted for reducing the floating point error.
        int EPOCH = 100000;

        SingleIndexBuilder(int w, List<List<Double>> scanner, IndexOperator indexOperator){
            this.scanner = scanner;
            this.w = w;

            t = new double[w * 2];
            buffer = new double[EPOCH];

            this.indexOperator = indexOperator;
        }

        void run() {
            Map<Integer,Map<Double, IndexNode>> indexNodeMap = new HashMap<>();
            int window = this.w;

            // step 1: fixed-width index rows
            for(int i = 0; i < scanner.size() ;i++){
                double sum = 0;
                for(int j = 0; j < scanner.get(i).size(); j++){
                    sum += scanner.get(i).get(j);
                    if((j + 1) % window == 0){
                        if(i == 0){
                            Map<Double,IndexNode> map = new HashMap<>();
                            indexNodeMap.put(j / window, map);
                        }
                        double toRound = MeanIntervalUtils.toRound(sum / window);
                        if(indexNodeMap.get(j / window).get(toRound) == null){

                            IndexNode idxNode = new IndexNode();
                            idxNode.getIDs().add(i);
                            indexNodeMap.get(j /window).put(toRound ,idxNode);
                        }
                        else {
                            indexNodeMap.get(j / window).get(toRound).getIDs().add(i);
                        }
                        sum = 0;
                    }
                }
            }
            for(Map.Entry<Integer,Map<Double,IndexNode>> entry : indexNodeMap.entrySet()){
                indexOperator.writeAll(entry.getKey() * window, (entry.getKey() + 1) * window - 1, entry.getValue());
            }
        }
    }
}
