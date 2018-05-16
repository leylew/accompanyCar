package cn.edu.fudan.dsm.MultiIndexes.common.entity;

import java.util.ArrayList;
import java.util.List;

public class IndexNode {

    public static int MAXIMUM_DIFF = 256;

    private List<Integer> ids;

    public IndexNode() {
        ids = new ArrayList<>(100);
    }

    public List<Integer> getIDs() { return ids;}
}