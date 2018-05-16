package cn.edu.fudan.dsm.MultiIndexes.Utils;

import java.util.concurrent.ThreadLocalRandom;

public class RandomUtils {

    /**
     * Generate random double number in [min, max]
     *
     * @param min the lower bound
     * @param max the upper bound
     * @return the random number in [min, max]
     */
    public static double random(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max + 0.00001);
    }

    /**
     * Generate random integer number in [min, max]
     *
     * @param min the lower bound
     * @param max the upper bound
     * @return the random number in [min, max]
     */
    public static int random(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }
}
