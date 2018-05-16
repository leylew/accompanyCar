package cn.edu.fudan.dsm.MultiIndexes.operator.file;

import cn.edu.fudan.dsm.MultiIndexes.common.entity.IndexNode;

import java.io.*;
import java.util.List;
import java.util.Map;

public class FileHandler implements Closeable {

    // common
    private String mode;
    private File file;

    long length = 0;

    // for read-only mode ("r")
    private RandomAccessFile reader;

    // for write-only mode ("w")
    private BufferedOutputStream writer;

    /**
     * Create the file i/o instance. Note that the instance can be either used to write or read.
     *
     * @param filePath the target file.
     * @param mode     i/o mode ("w": write, "r": read).
     */
    public FileHandler(String filePath, String mode) throws IOException {
        this.mode = mode;
        this.file = new File(filePath);
        if(!file.getParentFile().exists()){
            file.getParentFile().mkdirs();
            file.createNewFile();
            //System.out.println("create");
        }
        this.length = file.length();
        if(mode.equals("r"))reader = new RandomAccessFile(file,"r");
        else this.writer = new BufferedOutputStream(new FileOutputStream(file));
    }

    /**
     * Return the file instance to support advanced operations, e.g. get file size.
     *
     * @return the file instance associate with this handler
     */
    /*public File getFile() {
        return file;
    }*/

    /**
     * Seek to specific position of the file, and read the following number of bytes.
     *
     * @param pos           the start position
     * @param lengthOfBytes the number of bytes should be read (can not larger than INT_MAX = 2GB)
     * @return a byte array containing the bytes read
     * @throws IOException if any error occurred during the read process
     */
    public byte[] read(long pos, int lengthOfBytes) throws IOException {
        if (!mode.equals("r")) {
            throw new IllegalStateException("The file handler is not instanced for read.");
        }
        byte[] bytes = new byte[lengthOfBytes];
        reader.seek(pos);
        reader.read(bytes, 0, lengthOfBytes);
        return bytes;
    }

    /**
     * Append the bytes to the tail of the file.
     *
     * @param bytes the bytes need to be written
     * @throws IOException if any error occurred during the write process
     */
    public void write(byte[] bytes) throws IOException {
        if (!mode.equals("w")) {
            throw new IllegalStateException("The file handler is not instanced for write.");
        }
        writer.write(bytes, 0, bytes.length);
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        switch (mode) {
            case "r":
                reader.close();
                break;
            case "w":
                writer.close();
                break;
            default:
                break;
        }
    }

    public void writeToFile(Map<Double, IndexNode> indexNodeMap) {
        try {
            for(Map.Entry<Double,IndexNode> entry : indexNodeMap.entrySet()){
                String write = entry.getKey() + "";
                List<Integer> idList = entry.getValue().getIDs();
                for(int i = 0; i < idList.size(); i++){
                    write = write.concat(" " + idList.get(i));
                }
                write = write.concat("\n");
                try {
                    writer.write(write.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readAll() {
        try {
            this.reader = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        byte[] bytes = new byte[(int)this.length];
        try {
            FileInputStream fis = new FileInputStream(file);
            while ((length = fis.read(bytes)) != -1) {
            }
            fis.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }
}
