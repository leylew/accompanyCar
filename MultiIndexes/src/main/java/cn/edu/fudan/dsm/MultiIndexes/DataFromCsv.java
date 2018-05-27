package cn.edu.fudan.dsm.MultiIndexes;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DataFromCsv {
    public static void main(String[] args)
    {
        int seriesLength = 1000;
        File dir = new File("/home/lylw/temp files/MesoWest Data/BGBW");
        File[] files = dir.listFiles();
        System.out.println(files.length);
        BufferedReader br = null;
        for(File file : files){
            System.out.println(file.getName());
            try {
                br = new BufferedReader(new FileReader(file));
                String line = "";
                FileOutputStream fos = new FileOutputStream("files/data/weather_series", true);
                DataOutputStream dos = new DataOutputStream(fos);
                int lineCount = 0;
                List<String> seriesList = new ArrayList<>();
                while((line = br.readLine()) != null){
                    String[] array =   line.split(",");
                    if(lineCount  < 8){
                        lineCount ++;
                        continue;
                    }
                    if(array.length >= 3 && !array[2].equals("")){
                        lineCount ++;
                        seriesList.add(array[2]);
                    }

                    if(lineCount - 8 == seriesLength){
                        for(int i = 0; i < seriesLength; i++){
                            dos.writeDouble(Double.parseDouble(seriesList.get(i)));
                            System.out.println(seriesList.get(i));
                        }
                        lineCount = 8;
                        seriesList.clear();
                    }
                }
                dos.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
