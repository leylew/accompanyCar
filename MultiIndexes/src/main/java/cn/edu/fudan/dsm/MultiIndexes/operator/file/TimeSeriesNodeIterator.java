package cn.edu.fudan.dsm.MultiIndexes.operator.file;

import cn.edu.fudan.dsm.MultiIndexes.common.Pair;
import cn.edu.fudan.dsm.MultiIndexes.common.entity.TimeSeriesNode;

import java.io.*;
import java.util.Iterator;

public class TimeSeriesNodeIterator implements Iterator<Pair<Long, TimeSeriesNode>> {

//    private FileHandler fileHandler;

    private DataInputStream dis;

    private int fetchSize;

    private long length;

    private long pos;

    private Pair<Long, TimeSeriesNode> next = null;

    public TimeSeriesNodeIterator(File file, int fetchSize) throws FileNotFoundException {
//        this.fileHandler = fileHandler;
        this.dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        this.fetchSize = fetchSize;
        this.length = file.length();
//        this.length = fileHandler.getFile().length();
        this.pos = 0;
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            try {
                if (pos < length) {
                    byte[] bytes = new byte[fetchSize];
                    pos += dis.read(bytes);
//                    byte[] bytes = fileHandler.read(pos, fetchSize);
                    TimeSeriesNode node = new TimeSeriesNode();
                    node.parseBytes(bytes);
                    next = new Pair<>(pos + 1, node);
//                    pos += fetchSize;
                }
                return next != null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    @Override
    public Pair<Long, TimeSeriesNode> next() {
        if (!hasNext()) {
            return null;
        }

        Pair<Long, TimeSeriesNode> temp = next;
        next = null;
        return temp;
    }
}