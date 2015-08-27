package com.hughes.util.raf.compressed;

import android.annotation.TargetApi;
import android.os.Build;
import com.hughes.util.raf.Serializer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPOutputStream;

@TargetApi(Build.VERSION_CODES.KITKAT)
public class CompressedListWriter {
    
    public static <T> void write(final RandomAccessFile raf,
            final Collection<T> list, final Serializer<T> serializer, int chunkSize)
            throws IOException {
        assert chunkSize > 0;
        final long startLocation = raf.getFilePointer();
        raf.writeLong(0); // placeholder for tocPos

        final List<Long> chunkStartPositions = new ArrayList<Long>();
        final List<Integer> chunkLastElements = new ArrayList<Integer>();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dataOut = new DataOutputStream(baos);
        final List<Integer> elementStarts = new ArrayList<Integer>();
        int elementCount = 0;
        for (final T t : list) {
            if (baos.size() > chunkSize) {
                writeChunk(raf, baos, elementStarts, elementCount, chunkStartPositions,
                           chunkLastElements);
            }

            elementStarts.add(baos.size());
            serializer.write(dataOut, t);
            dataOut.flush();
            ++elementCount;
        }
        writeChunk(raf, baos, elementStarts, elementCount, chunkStartPositions, chunkLastElements);

        final long tocPos = raf.getFilePointer();

        DataOutputStream tocOut = new DataOutputStream(new GZIPOutputStream(
                Channels.newOutputStream(raf.getChannel()), true));
        tocOut.writeInt(chunkStartPositions.size());
        for (int i = 0; i < chunkStartPositions.size(); ++i) {
            tocOut.writeLong(chunkStartPositions.get(i));
            tocOut.writeInt(chunkLastElements.get(i));
        }
        tocOut.flush();

        long endPos = raf.getFilePointer();
        raf.seek(startLocation);
        raf.writeLong(tocPos);
        raf.seek(endPos);
    }

    private static void writeChunk(final RandomAccessFile raf, 
            final ByteArrayOutputStream uncompressedBytes,
            final List<Integer> elementStarts, int endElement,
            final List<Long> chunkStartPositions, List<Integer> chunkLastElements)
            throws IOException {
        chunkStartPositions.add(raf.getFilePointer());
        chunkLastElements.add(endElement);

        ByteArrayOutputStream compressedBytes = new ByteArrayOutputStream();
        DataOutputStream chunkOut = new DataOutputStream(new GZIPOutputStream(compressedBytes));
        chunkOut.writeInt(elementStarts.size());
        for (final Integer elementStart : elementStarts) {
            chunkOut.writeInt(elementStart);
        }
        final byte[] bytes = uncompressedBytes.toByteArray();
        chunkOut.writeInt(bytes.length);
        chunkOut.write(bytes);
        chunkOut.close();
        
        raf.write(compressedBytes.toByteArray());

        elementStarts.clear();
        uncompressedBytes.reset();
    }

    
}
