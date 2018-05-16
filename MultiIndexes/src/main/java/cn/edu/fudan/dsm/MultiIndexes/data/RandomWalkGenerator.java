package cn.edu.fudan.dsm.MultiIndexes.data;

import cn.edu.fudan.dsm.MultiIndexes.Utils.RandomUtils;

public class RandomWalkGenerator implements SeriesGenerator {

    private double minStart;
    private double maxStart;

    private double minStep;
    private double maxStep;

    public RandomWalkGenerator(double minStart, double maxStart, double minStep, double maxStep) {
        this.minStart = minStart;
        this.maxStart = maxStart;
        this.minStep = minStep;
        this.maxStep = maxStep;
    }

    public double[] generate(int length) {
        double[] timeSeries = new double[length];
        timeSeries[0] = RandomUtils.random(minStart, maxStart);

        for (int i = 1; i < timeSeries.length; i++) {
            double sign = Math.random() < 0.5 ? -1 : 1;
            double step = RandomUtils.random(minStep, maxStep);
            timeSeries[i] = timeSeries[i - 1] + sign * step;
        }
        return timeSeries;
    }
}
