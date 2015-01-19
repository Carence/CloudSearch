import java.io.*;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Created by Clarence_Pz on 2015/1/17.
 */

public class RAOMStreamQueue <T extends Serializable> {

    private  final static Logger logger = Logger.getLogger(RAOMStreamQueue.class);

    //这两个参数希望读配置文件获取
    private static String path = "./";
    private static String filename = "FileQueue";

    private static final int SIZEOFINT = 4;
    private static final int EMPTYQUEUETYPE = -1;
    public static final int PAGE_SIZE = 64 * 1024 * 1024;
    private static final String data_suffix = ".data";
    private static final String index_suffix = ".index";

    //这里约定块标志为非负数，如果为负数标识读写指针相遇
    private RandomAccessFile rwFile;
    private RandomAccessFile indexFile;
    private FileChannel rwChannel;
    private FileChannel indexChannel;
    private MappedByteBuffer rwMbb;
    private MappedByteBuffer indexMbb;

    private static RAOMStreamQueue Instance = new RAOMStreamQueue();

    private RAOMStreamQueue(){
        if (path == null || path.trim().length() == 0) {
            throw new IllegalArgumentException("filename illegal");
        }
        if (!path.endsWith("/")) {
            path += File.separator;
        }
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        try {
            indexFile = new RandomAccessFile(path + filename + index_suffix,"rw");
            indexChannel = indexFile.getChannel();
            indexMbb = indexChannel.map(READ_WRITE, 0, 2*SIZEOFINT);

            rwFile = new RandomAccessFile(path + filename + data_suffix,"rw");
            rwChannel = rwFile.getChannel();
            rwMbb = rwChannel.map(READ_WRITE, 0,rwChannel.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void reset(){
        logger.info("the file queue is empty and it will be reseted.");
        try {
            if(rwMbb.capacity() <= PAGE_SIZE){
                rwMbb.clear();
            }
            else {
                unmap(rwMbb);
                closeResource(rwFile);
                closeResource(rwChannel);

                rwFile = new RandomAccessFile(path + filename + data_suffix, "rw");
                rwFile.setLength(0);
                rwChannel = rwFile.getChannel();
                rwMbb = rwChannel.map(READ_WRITE, 0, PAGE_SIZE);
            }
            setReadIndex(0);
            setWriteIndex(0);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public synchronized void clear(){
        reset();
    }

    private void resize(){
        logger.info("the file queue is full and resize it.");
        try {
            rwMbb = rwChannel.map(READ_WRITE, 0, PAGE_SIZE + rwChannel.size());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static RAOMStreamQueue getInstance() {
        if(Instance==null) Instance = new RAOMStreamQueue();
        return Instance;
    }

    public synchronized void push(T item) {
        if (item == null) {
            logger.error("empty message");
            return;
        }

        byte[] contents = new byte[0];
        try {
            contents = toBytes(item);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        int length = contents.length;
        int writePos = getWriteIndex();

        // if reach the button of the filequeue
        if (0 == rwMbb.remaining() || writePos + length + SIZEOFINT >= PAGE_SIZE) {
            logger.info("reach the button of the filequeue, resize rw buffer.");
            resize();
        }

        rwMbb.position(writePos);
        rwMbb.putInt(length);
        writePos += SIZEOFINT;
        setWriteIndex(writePos);

        rwMbb.position(writePos);
        rwMbb.put(contents);
        writePos += length;
        setWriteIndex(writePos);
        return;
    }

    public synchronized T pull() throws Exception {
        int readPos = getReadIndex();
        int writePos = getWriteIndex();
        if(readPos == writePos){
            logger.info("touch the button of filequeue.");
            reset();
            return null;
        }
        rwMbb.position(readPos);
        int length = rwMbb.getInt();
        readPos += SIZEOFINT;
        setReadIndex(readPos);
        byte[] contents = new byte[length];
        rwMbb.position(readPos);
        rwMbb.get(contents);
        readPos += length;
        setReadIndex(readPos);
        T object = toObject(contents);
        return object;
    }

    private void setReadIndex(int readPos){
        indexMbb.position(0);
        indexMbb.putInt(readPos);
    }

    private int getReadIndex(){
        indexMbb.position(0);
        return indexMbb.getInt();
    }

    private void setWriteIndex(int writePos){
        indexMbb.position(SIZEOFINT);
        indexMbb.putInt(writePos);
    }

    private int getWriteIndex(){
        indexMbb.position(SIZEOFINT);
        return indexMbb.getInt();
    }

    private T toObject(byte[] content) throws IOException,
        ClassNotFoundException {
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;
        try {
            bais = new ByteArrayInputStream(content);
            ois = new ObjectInputStream(bais);
            return (T) ois.readObject();
        } finally {
            closeResource(bais);
            closeResource(ois);
        }
    }

    private byte[] toBytes(T item) throws IOException {
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject((Object) item);
            oos.flush();
            return baos.toByteArray();
        } finally {
            closeResource(baos);
            closeResource(oos);
        }
    }

    private void unmap(ByteBuffer cb)
    {
        if (!cb.isDirect()) return;

        // we could use this type cast and call functions without reflection code,
        // but static import from sun.* package is risky for non-SUN virtual machine.
        //try { ((sun.nio.ch.DirectBuffer)cb).cleaner().clean(); } catch (Exception ex) { }

        try {
            Method cleaner = cb.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            clean.setAccessible(true);
            clean.invoke(cleaner.invoke(cb));
        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        cb = null;
    }

    private void closeResource(Closeable c) throws IOException {
        if (c != null) {
            c.close();
        }
    }
}
