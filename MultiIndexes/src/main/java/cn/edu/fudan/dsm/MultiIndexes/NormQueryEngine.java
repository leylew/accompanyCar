package cn.edu.fudan.dsm.MultiIndexes;

import cn.edu.fudan.dsm.MultiIndexes.Utils.MeanIntervalUtils;
import cn.edu.fudan.dsm.MultiIndexes.common.Pair;
import cn.edu.fudan.dsm.MultiIndexes.common.QuerySegment;
import cn.edu.fudan.dsm.MultiIndexes.operator.IndexOperator;
import cn.edu.fudan.dsm.MultiIndexes.operator.TimeSeriesOperator;
import cn.edu.fudan.dsm.MultiIndexes.operator.file.IndexFileOperator;
import cn.edu.fudan.dsm.MultiIndexes.operator.file.TimeSeriesFileOperator;

import java.io.IOException;
import java.util.*;

public class NormQueryEngine {

    private static final int[] WuList = {25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 800};
    private static final boolean[] WuEnabled = {true, true, false, true, false, false, false, true, false, false, false, false, false, false, false, true, true};

    private TimeSeriesOperator timeSeriesOperator = null;
    private IndexOperator[] indexOperators = new IndexOperator[WuList.length];
    private List<List<Pair<Double, Pair<Integer, Integer>>>> statisticInfos = new ArrayList<>(WuList.length);
    private int queryLength, cntScans;
    private String filePath;
    private int seriesLength;

    public static void main(String args[]) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Data file = ");
        String file = scanner.nextLine();

        System.out.print("Data Length = ");
        int n = scanner.nextInt();

        System.out.print("Series Length = ");
        int n1 = scanner.nextInt();

        NormQueryEngine queryEngine = new NormQueryEngine(file, n, n1, "file");

        do {
            int offset, length;
            double epsilon, alpha, beta;
            do {
                System.out.print("Offset = ");
                offset = scanner.nextInt();
                if (offset > n) {
                    System.out.println("Invalid! Offset shouldn't be larger than " + n + ".");
                }
            } while (offset > n);
            //if (offset <= 0) break;  // exit
            do {
                System.out.print("Length = ");
                length = scanner.nextInt();
                if (length < WuList[0]) {
                    System.out.println("Invalid! Length shouldn't be smaller than " + WuList[0] + ".");
                }
                if (offset + length - 1 > n) {
                    System.out.println("Invalid! Offset+Length-1 shouldn't be larger than " + n + ".");
                }
            } while (length < WuList[0] || offset + length - 1 > n);
            do {
                System.out.print("Epsilon = ");
                epsilon = scanner.nextDouble();
                if (epsilon < 0.0) {
                    System.out.println("Invalid! Epsilon shouldn't be smaller than 0.0.");
                }
            } while (epsilon < 0.0);
            do {
                System.out.print("Alpha = ");
                alpha = scanner.nextDouble();
                if (alpha <= 0.0) {
                    System.out.println("Invalid! Alpha should be greater than 0.0.");
                }
            } while (alpha <= 0.0);
            do {
                System.out.print("Beta = ");
                beta = scanner.nextDouble();
                if (beta == 0.0) {
                    System.out.println("Invalid! Beta should not equal to 0.0.");
                }
            } while (beta == 0.0);

            // initialization: 0-T, 1-T_1, 2-T_2, 3-#candidates, 4-#answers

            // execute the query request
            for(int i = 0; i< WuList.length && WuList[i] <= length; i ++){
                //System.out.println("window " + WuList[i] + " " + WuEnabled[i]);
                if(!WuEnabled[i])continue;
                queryEngine.query(offset, length, WuList[i], i, epsilon, alpha, beta);
            }

        } while (true);
    }

    public NormQueryEngine(String filePath, int n, int n1, String storageType) throws IOException {
        this.queryLength = n;
        this.seriesLength = n1;
        switch (storageType) {
            case "file":
                timeSeriesOperator = new TimeSeriesFileOperator(filePath, n, n,false);
                break;
            case "hdfs":
                break;
        }
        for (int i = 0; i < WuList.length; i++) {
            if (!WuEnabled[i]) continue;
            switch (storageType) {
                case "file":
                    indexOperators[i] = new IndexFileOperator("standard", WuList[i], false);
                    break;
                case "hdfs":
                    break;
            }

        }
    }

    @SuppressWarnings("unchecked")
    public boolean query(int offset, int length, int window, int level, double epsilon, double alpha, double beta) throws IOException {
        // fetch corresponding subsequence from data series
        List<Double> queryData = timeSeriesOperator.readTimeSeries(offset, length);
        System.out.println(queryData);
        System.out.println("query data size "+window);
        return query(queryData, offset, offset + length - 1, window, level, epsilon, alpha, beta);
    }

    public boolean query(List<Double> queryData, int ts, int te, int window, int level, double epsilon, double alpha, double beta) throws IOException {
        // initialization: clear cache
        cntScans = 0;
        int queryLength = queryData.size();

        // Phase 0: calculate statistics for the query series
        // calculate mean and std of whole query series
        double ex = 0, ex2 = 0;
        for (Double value : queryData) {
            ex += value;
            ex2 += value * value;
        }
        double meanQ = ex / queryLength;
        double stdQ = Math.sqrt(ex2 / queryLength - meanQ * meanQ);
        System.out.println("mean stdQ: "  + meanQ + " " + stdQ);

        List<QuerySegment> queries = splitQuerySeries(queryData, ts, te, window);

        // Phase 1: index-probing
        Map<Integer,Integer> rIds = new HashMap<>();
        for (int i = 0; i < queries.size(); i++) {
            QuerySegment query = queries.get(i);
            // query possible rows which mean is in distance range of i-th disjoint window
            double beginRound = 1.0 / alpha * query.getMean() + (1 - 1.0 / alpha) * meanQ - beta - Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / query.getWu());
            double beginRound1 = alpha * query.getMean() + (1 - alpha) * meanQ - beta - Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / query.getWu());
            //beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1));
            beginRound = Math.min(beginRound, beginRound1);

            double endRound = alpha * query.getMean() + (1 - alpha) * meanQ + beta + Math.sqrt(alpha * alpha * stdQ * stdQ * epsilon * epsilon / query.getWu());
            double endRound1 = 1.0 / alpha * query.getMean() + (1 - 1.0 / alpha) * meanQ + beta + Math.sqrt(1.0 / (alpha * alpha) * stdQ * stdQ * epsilon * epsilon / query.getWu());
            //endRound = MeanIntervalUtils.toRound(Math.max(endRound, endRound1));
            endRound = Math.max(endRound, endRound1);

            List<Integer> valid = scanIndex(beginRound, endRound, query, level);
            for(int j = 0; j < valid.size(); j++){
                if(rIds.get(valid.get(j)) == null)rIds.put(valid.get(j), 1);
                else rIds.put(valid.get(j), rIds.get(valid.get(j)) + 1);
            }
        }
        List<Integer> finalList = new ArrayList<>();
        for(Map.Entry<Integer, Integer> entry : rIds.entrySet()){
            if(entry.getValue() >= queries.size()) {
                //System.out.println("finalList contain: " + entry.getKey());
                finalList.add(entry.getKey());
            }
        }
        System.out.println("final list length:" + finalList.size());
        int finalCount = 0;
        for(int i = 0; i < finalList.size(); i++){
            int offset = finalList.get(i);
            List<Double> tss = timeSeriesOperator.readTimeSeries(offset * seriesLength + ts, te - ts + 1);
            if(timeSeriesOperator.TsNormED(tss, queryData, alpha, beta) <= epsilon){
                finalCount ++;
            }
        }
        System.out.println("finalCount: " + finalCount);
        return true;
    }

    private List<Integer> scanIndex(double beginRound, double endRound, QuerySegment query, int level) {
        List<Integer> retIds =  new ArrayList<>();
        try {
            retIds = indexOperators[level].readIndexes(beginRound, endRound, query.getIndexBegin(), query.getIndexEnd());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return retIds;
    }

    private List<QuerySegment> splitQuerySeries(List<Double> queryData, int ts, int te, int window) {
        List<QuerySegment> querySegments = new ArrayList<>();
        double mean = 0;
        for(int i = ts;i <= te; i ++){
            if(i % window == 0){
                int j = 0;
                for(;j < window && i + j <= te; j ++){
                    mean += queryData.get(i + j - ts);
                }
                if(j < window)break;

                QuerySegment qs = new QuerySegment(mean / window, i, i + window - 1, 0, 0, window);
                querySegments.add(qs);
                mean = 0;
                i = i + window - 1;
            }
        }
        return querySegments;
    }
}
