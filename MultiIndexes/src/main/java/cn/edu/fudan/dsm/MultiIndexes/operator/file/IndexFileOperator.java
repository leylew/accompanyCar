package cn.edu.fudan.dsm.MultiIndexes.operator.file;

import cn.edu.fudan.dsm.MultiIndexes.common.Pair;
import cn.edu.fudan.dsm.MultiIndexes.common.entity.IndexNode;
import cn.edu.fudan.dsm.MultiIndexes.operator.IndexOperator;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexFileOperator implements IndexOperator {

    //private FileHandler fileHandler;
    private String dirPath;

    public IndexFileOperator(String type, int Wu, boolean rebuild) throws IOException {
        String dirPath = "files" + File.separator + "index-" + (type.equals("standard") ? "" : type) + "-" + Wu + File.separator;
        this.dirPath = dirPath;
        //fileHandler = new FileHandler(dirPath, rebuild ? "w" : "r");
    }


    @Override
    public List<Integer> readIndexes(double keyFrom, double keyTo, int indexBegin, int indexEnd) throws IOException {
        File readFile = new File(dirPath + indexBegin + "_" + indexEnd);
        FileInputStream fis = new FileInputStream(readFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
        double l, r;
        List<Integer> rIds = new ArrayList<>();
        String index;
        while((index = bufferedReader.readLine()) != null){
            String[] ids = index.split(" ");
            l = Double.parseDouble(ids[0]);
            r = l + 0.5;
            //System.out.print("( l: " + l + " | " + "r: "+r + " ) ");
            if((keyTo >= r) && (keyFrom <= r)){
                for(int j = 1; j < ids.length; j++){
                    rIds.add(Integer.parseInt(ids[j]));
                    //System.out.print(ids[j]+ " ");
                }
            }
            else if(keyTo >= l && keyFrom <= l){
                for(int j = 1; j < ids.length; j++){
                    rIds.add(Integer.parseInt(ids[j]));
                    //System.out.print(ids[j]+ " ");
                }
            }
            //System.out.print("\n");
        }
        bufferedReader.close();
        fis.close();
        return rIds;
    }

    @Override
    public List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() throws IOException {
        return null;
    }

    private int lowerBound(double keyFrom) throws IOException {  // left range, return index of List<Integer> offsets

        return -1;
    }

    private int upperBound(double keyTo) throws IOException {  // right range, return index of List<Integer> offsets

        return -1;
    }

    @Override
    public void writeAll(Map<Double, IndexNode> sortedIndexes, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException {

    }

    @Override
    public void writeAll(Integer St, int Ed, Map<Double, IndexNode> indexNodeMap) {
        FileHandler fileHandler = null;
        try {
            fileHandler = new FileHandler(dirPath + System.getProperty("file.separator") + St + "_" + Ed, "standard");
        } catch (IOException e) {
            e.printStackTrace();
        }
        fileHandler.writeToFile(indexNodeMap);
    }


    private void writeIndexes(Map<Double, IndexNode> sortedIndexes) throws IOException {
        for (Map.Entry<Double, IndexNode> entry : sortedIndexes.entrySet()) {
            writeIndex(entry.getKey(), entry.getValue());
        }
    }

    private void writeIndex(double key, IndexNode value) throws IOException {

    }

    @Override
    public void close() throws IOException {
        //fileHandler.close();
    }
}
