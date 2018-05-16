package cn.edu.fudan.dsm.MultiIndexes.data;

import cn.edu.fudan.dsm.MultiIndexes.Utils.RandomUtils;

public class SineGenerator implements SeriesGenerator {

    private double minFrequency;
    private double maxFrequency;

    private double minAmplitude;
    private double maxAmplitude;

    private double minMean;
    private double maxMean;

    public SineGenerator(double minFrequency, double maxFrequency, double minAmplitude, double maxAmplitude, double minMean, double maxMean) {
        this.minFrequency = minFrequency;
        this.maxFrequency = maxFrequency;
        this.minAmplitude = minAmplitude;
        this.maxAmplitude = maxAmplitude;
        this.minMean = minMean;
        this.maxMean = maxMean;
    }

    public double[] generate(int length) {
        double[] timeSeries = new double[length];
        double frequency = RandomUtils.random(minFrequency, maxFrequency);
        double amplitude = RandomUtils.random(minAmplitude, maxAmplitude);
        double mean = RandomUtils.random(minMean, maxMean);
        double phase = RandomUtils.random(0, 2 * Math.PI);

        for (int i = 0; i < timeSeries.length; i++) {
            timeSeries[i] = mean + amplitude * Math.sin(2 * i * (Math.PI / timeSeries.length) * frequency + phase) + RandomUtils.random(amplitude * 0.05 * -1, amplitude * 0.05);  // add noise
        }
        return timeSeries;
    }
}

