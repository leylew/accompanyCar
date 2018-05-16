package cn.edu.fudan.dsm.MultiIndexes.operator;

import cn.edu.fudan.dsm.MultiIndexes.common.Pair;
import cn.edu.fudan.dsm.MultiIndexes.common.entity.IndexNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IndexOperator extends Closeable {

    /**
     * Read index rows from the index file.
     *
     * @param keyFrom the lower-bound of the key (inclusive)
     * @param keyTo   the upper-bound of the key (inclusive)
     * @return index rows whose keys in the specified range
     * @throws IOException if any error occurred during reading process
     */
    List<Integer> readIndexes(double keyFrom, double keyTo, int indexBegin, int indexEnd) throws IOException;

    /**
     * Read the statistic information (metadata) of the index, e.g. the number of offsets and intervals in every row.
     *
     * @return statistic information (metadata) of this index
     * @throws IOException if any error occurred during reading process
     */
    List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() throws IOException;

    /**
     * Write the whole index structure to index file.
     * Including the index rows, statistic information (metadata) and offset information used to random access index rows.
     *
     * @param sortedIndexes the index rows ordered by the key in ascending order
     * @param statisticInfo the statistic information (metadata) corresponding to the index
     * @throws IOException if any error occurred during write process
     */
    void writeAll(Map<Double, IndexNode> sortedIndexes, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException;


    void writeAll(Integer key, int i, Map<Double, IndexNode> indexNodeMap);
}