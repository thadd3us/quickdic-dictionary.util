package com.hughes.util.raf.compressed;

import android.annotation.TargetApi;
import android.os.Build;

import com.hughes.util.raf.Serializer;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@TargetApi(Build.VERSION_CODES.KITKAT)
public class CompressedListTest extends TestCase {

    public void testEmpty() throws IOException {
        List<Long> list = new ArrayList<Long>();

        final File file = File.createTempFile("testEmpty", "");
        file.deleteOnExit();
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        
        CompressedListWriter.write(raf, list, Serializer.LONG, 333);

        CompressedList<Long> compressedList = CompressedList.create(raf, Serializer.LONG, 0);
        assertEquals(list.size(), compressedList.size());
    }
    
    public void testOneElement() throws IOException {
        List<Long> list = new ArrayList<Long>();
        list.add(1l);
        
        final File file = File.createTempFile("testOneElement", "");
        file.deleteOnExit();
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        
        CompressedListWriter.write(raf, list, Serializer.LONG, 333);
        
        CompressedList<Long> compressedList = CompressedList.create(raf, Serializer.LONG, 0);
        assertEquals(list.size(), compressedList.size());
        for (int i = 0; i < list.size(); ++i) {
            assertEquals(list.get(i), compressedList.get(i));
        }
    }
    
    public void testTwoElementsTwoChunks() throws IOException {
        List<Long> list = new ArrayList<Long>();
        list.add(1l);
        list.add(7l);
        
        final File file = File.createTempFile("testOneElement", "");
        file.deleteOnExit();
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        
        CompressedListWriter.write(raf, list, Serializer.LONG, 1);
        
        CompressedList<Long> compressedList = CompressedList.create(raf, Serializer.LONG, 0);
        assertEquals(list.size(), compressedList.size());
        for (int i = 0; i < list.size(); ++i) {
            assertEquals(list.get(i), compressedList.get(i));
        }
    }
    
    public void testTwoElementsOneChunk() throws IOException {
        List<Long> list = new ArrayList<Long>();
        list.add(1l);
        list.add(7l);
        
        final File file = File.createTempFile("testOneElement", "");
        file.deleteOnExit();
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        
        CompressedListWriter.write(raf, list, Serializer.LONG, 1000);
        
        CompressedList<Long> compressedList = CompressedList.create(raf, Serializer.LONG, 0);
        assertEquals(list.size(), compressedList.size());
        for (int i = 0; i < list.size(); ++i) {
            assertEquals(list.get(i), compressedList.get(i));
        }
    }

    public void testRandom() throws IOException {
        Random random = new Random(0);
        List<Long> list = new ArrayList<Long>();
        for (int i = 0; i < 10000; ++i) {
            list.add((long) random.nextInt(512));
        }
        
        final File file = File.createTempFile("testRandom", "");
        file.deleteOnExit();
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        CompressedListWriter.write(raf, list, Serializer.LONG, 777);
        assertEquals(47637, raf.getFilePointer());
        
        CompressedList<Long> compressedList = CompressedList.create(raf, Serializer.LONG, 0);
        assertEquals(1236, compressedList.tocBytes());
        assertEquals(list.size(), compressedList.size());
        for (int i = 0; i < 1000; ++i) {
            final int position = random.nextInt(list.size());
            assertEquals(list.get(position), compressedList.get(position));
        }
        
        for (int i = 0; i < list.size(); ++i) {
            assertEquals(list.get(i), compressedList.get(i));
        }
    }

}
