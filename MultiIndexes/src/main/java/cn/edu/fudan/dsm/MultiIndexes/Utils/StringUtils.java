package cn.edu.fudan.dsm.MultiIndexes.Utils;

public class StringUtils {

    /**
     * Generate fixed length string from integer
     *
     * @param x     the integer want to parse to string
     * @param width desired length of the output string
     * @return the length-width string from x
     */
    public static String toStringFixedWidth(long x, int width) {
        String str = String.valueOf(x);
        if (str.length() > width) {
            throw new IllegalArgumentException("width is too short (x: " + str + ", width: " + width + ")");
        }
        StringBuilder sb = new StringBuilder(str);
        for (int i = str.length(); i < width; i++) {
            sb.insert(0, "0");
        }
        return sb.toString();
    }
}
