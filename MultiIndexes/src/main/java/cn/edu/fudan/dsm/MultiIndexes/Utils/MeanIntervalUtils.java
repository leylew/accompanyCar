package cn.edu.fudan.dsm.MultiIndexes.Utils;

import cn.edu.fudan.dsm.MultiIndexes.common.Pair;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MeanIntervalUtils {

    /**
     * The minumum value of time series, which should be adapt to the real data.
     */
    public static double MINIMUM = 1000000;

    /**
     * Precise d to 0.5*10^(-x+1).
     * For example: x=1 -> d=0.5, x=2 -> d=0.05, etc.
     */
    public static int posOfD = 3;

    /**
     * Round float number to half integer. d = 0.5
     * For Example: 1.9 ->  1.5,  1.4 ->  1.0,  1.5 ->  1.5
     * -1.9 -> -2.0, -1.4 -> -1.5, -1.5 -> -1.5
     *
     * @param value should be rounded
     * @return rounded value
     */
    public static double toRound(double value) {
        value *= Math.pow(10, posOfD - 1);
        int intValue = (int) Math.floor(value);
        int retValue = intValue;
        retValue -= intValue % 5;
        if(retValue > value) retValue -= 5;
        return retValue * Math.pow(10, -posOfD + 1);
        // return (intValue * 1.0 + addValue) * Math.pow(10, -posOfD+1);
    }

    /**
     * toRound based on statistic information (lower bound)
     *
     * @param value         should be rounded
     * @param statisticInfo statistic information of index table
     * @return rounded value based on statistic information
     */
    public static double toRound(double value, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) {
        double rounded = toRound(value);
        int index = Collections.binarySearch(statisticInfo, new Pair<>(rounded, 0), Comparator.comparing(Pair::getFirst));
        if (index < 0) {
            index = -(index + 1) - 1;
            if (index < 0) return rounded - 10000;
            return statisticInfo.get(index).getFirst();
        } else {
            return rounded;
        }
    }

    /**
     * To upper bound of mean interval.
     * For example: 1.0 ->  1.5,  1.5 ->  2.0
     * -1.0 -> -0.5, -1.5 -> -1.0
     *
     * @param round mean interval round
     * @return upper bound
     */
    public static double toUpper(double round) {
        round *= Math.pow(10, posOfD-1);
        round += 0.5;
        round *= Math.pow(10, -posOfD+1);
        return round;
    }

    /**
     * toUpper based on statistic information (upper bound)
     *
     * @param round         mean interval round
     * @param statisticInfo statistic information of index table
     * @return upper bound basedo on statistic information
     */
    public static double toUpper(double round, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) {
        double rounded = toUpper(round);
        int index = Collections.binarySearch(statisticInfo, new Pair<>(rounded, 0), Comparator.comparing(Pair::getFirst));
        if (index < 0) {
            index = -(index + 1);
            if (index >= statisticInfo.size()) return rounded + 10000;
            return statisticInfo.get(index).getFirst();
        } else {
            return rounded;
        }
    }

    /**
     * Convert mean value into HBase row key.
     * 1. Add the minimum value, and convert to positive number;
     * 2. Round to 0.0 or 0.5;
     * 3. Convert to byte array.
     *
     * @param value should be processed
     * @return row key in bytes
     */
    public static byte[] toRoundBytes(double value) {
        return Bytes.toBytes(toRound(value) + MINIMUM);
    }

    public static byte[] toBytes(double value) {
        return Bytes.toBytes(value + MINIMUM);
    }

    /**
     * Convert HBase row key into mean value.
     * 1. Convert to double value;
     * 2. Minus the minimum value.
     *
     * @param bytes
     * @return
     */
    public static double toDouble(byte[] bytes) {
        return Bytes.toDouble(bytes) - MINIMUM;
    }

    /**
     * Convert mean value to Kudu row key.
     * @param value mean value in double
     * @return row key in long
     */
    public static long toLong(double value) {
        return (long) (value * Math.pow(10, posOfD));
    }

    /**
     * Convert Kudu row key into mean value
     * @param longValue row key in long
     * @return mean value in double
     */
    public static double toDouble(long longValue) {
        return 1.0 * longValue / Math.pow(10, posOfD);
    }
}
