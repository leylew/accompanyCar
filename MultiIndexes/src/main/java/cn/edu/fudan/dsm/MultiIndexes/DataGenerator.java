package cn.edu.fudan.dsm.MultiIndexes;

import cn.edu.fudan.dsm.MultiIndexes.Utils.RandomUtils;
import cn.edu.fudan.dsm.MultiIndexes.data.GaussianGenerator;
import cn.edu.fudan.dsm.MultiIndexes.data.RandomWalkGenerator;
import cn.edu.fudan.dsm.MultiIndexes.data.SeriesGenerator;
import cn.edu.fudan.dsm.MultiIndexes.data.SineGenerator;
import org.apache.commons.io.FileUtils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DataGenerator {

    private static final String DATA_FILENAME_PREFIX = "files" + File.separator + "data" + File.separator;

    private List<SeriesGenerator> generators = new ArrayList<>();
    private int dataLength;

    public DataGenerator(int dataLength) throws IOException {
        this.dataLength = dataLength;
    }

    public static void main(String args[]) throws IOException {
        System.out.print("Data Length = ");
        Scanner scanner = new Scanner(System.in);
        int dataLength = scanner.nextInt();
        System.out.print("Generate data? [true/false] = ");
        boolean generateData = scanner.nextBoolean();

        scanner.close();

        DataGenerator generator = new DataGenerator(dataLength);
        if (generateData) {
            generator.generateSyntheticDataToFile();
        }
    }

    private double[] generateSegment(int maxLength) {
        int t = RandomUtils.random(0, generators.size() - 1);
        int l = RandomUtils.random(Math.min(1000, maxLength), maxLength);
        SeriesGenerator seriesGenerator = generators.get(t);
        return seriesGenerator.generate(l);
    }

    private void generateSyntheticDataToFile() {
        RandomWalkGenerator randomWalkGenerator = new RandomWalkGenerator(-5, 5, 0, 1);
        generators.add(randomWalkGenerator);
        GaussianGenerator gaussianGenerator = new GaussianGenerator(-5, 5, 0, 2);
        generators.add(gaussianGenerator);
        SineGenerator sineGenerator = new SineGenerator(2, 10, 2, 10, -5, 5);
        generators.add(sineGenerator);

        File file = new File(DATA_FILENAME_PREFIX + dataLength);
        try {
            FileUtils.forceMkdirParent(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileOutputStream fos = new FileOutputStream(file)) {
            DataOutputStream dos = new DataOutputStream(fos);
            int left = dataLength;
            while (left > 0) {
                // generate the following segment by the generator
                double[] segment = generateSegment(Math.min(left, dataLength / 100));
                left -= segment.length;

                // write the segment to file
                for (double data : segment) {
                    dos.writeDouble(data);
                }
            }
            dos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
