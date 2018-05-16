package cn.edu.fudan.dsm.MultiIndexes.common;

public class NormInterval {

    private int left;

    private int right;

    private double exLower;

    private double ex2Lower;

    private double exUpper;

    private double ex2Upper;

    public NormInterval(int left, int right, double exLower, double ex2Lower, double exUpper, double ex2Upper) {
        this.left = left;
        this.right = right;
        this.exLower = exLower;
        this.ex2Lower = ex2Lower;
        this.exUpper = exUpper;
        this.ex2Upper = ex2Upper;
    }

    @Deprecated
    public NormInterval(int left, int right, double ex, double ex2) {
        this.left = left;
        this.right = right;
        this.exLower = ex;
        this.ex2Lower = ex2;
    }

    public int getLeft() {
        return left;
    }

    public void setLeft(int left) {
        this.left = left;
    }

    public int getRight() {
        return right;
    }

    public void setRight(int right) {
        this.right = right;
    }

    @Deprecated
    public double getEx() {
        return exLower;
    }

    @Deprecated
    public double getEx2() {
        return ex2Lower;
    }

    public double getExLower() {
        return exLower;
    }

    public void setExLower(double exLower) {
        this.exLower = exLower;
    }

    public double getEx2Lower() {
        return ex2Lower;
    }

    public void setEx2Lower(double ex2Lower) {
        this.ex2Lower = ex2Lower;
    }

    public double getExUpper() {
        return exUpper;
    }

    public void setExUpper(double exUpper) {
        this.exUpper = exUpper;
    }

    public double getEx2Upper() {
        return ex2Upper;
    }

    public void setEx2Upper(double ex2Upper) {
        this.ex2Upper = ex2Upper;
    }

    @Override
    public String toString() {
        return "\n[" + String.valueOf((left-1)*50+1) + ", " + String.valueOf((right-1)*50+1) + "] - Ex: " + exLower + "-" + exUpper + ", Ex2: " + ex2Lower + "-" + ex2Upper;
    }
}
