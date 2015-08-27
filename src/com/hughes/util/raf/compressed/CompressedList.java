// Copyright 2011 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.hughes.util.raf.compressed;

import com.hughes.util.raf.Serializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.channels.Channels;
import java.util.AbstractList;
import java.util.HashMap;
import java.util.Map;
import java.util.RandomAccess;
import java.util.zip.GZIPInputStream;

public class CompressedList<T> extends AbstractList<T> implements RandomAccess {

    final RandomAccessFile raf;
    final Serializer<T> serializer;

    final long[] chunkStartPos;
    final int[] chunkLastElements;
    final Map<Integer, SoftReference<ChunkContents>> chunkContentsMap = new HashMap<Integer, SoftReference<ChunkContents>>();

    private static class ChunkContents {
        int firstElement;
        int[] elementStartPositions;
        byte[] decompressedBytes;
    }

    private DataInputStream getInputStream() throws IOException {
        return new DataInputStream(new GZIPInputStream(Channels.newInputStream(raf.getChannel())));
    }
    
    int numChunks() {
        return chunkStartPos.length;
    }
    
    int tocBytes() {
        return chunkStartPos.length * 12;
    }

    public CompressedList(final RandomAccessFile raf,
            final Serializer<T> serializer, final long startOffset)
            throws IOException {
        synchronized (raf) {
            this.raf = raf;
            this.serializer = serializer;
            raf.seek(startOffset);
            long tocPos = raf.readLong();

            raf.seek(tocPos);
            DataInputStream tocIn = getInputStream();
            final int numChunks = tocIn.readInt();
            chunkStartPos = new long[numChunks];
            chunkLastElements = new int[numChunks];

            for (int c = 0; c < numChunks; ++c) {
                chunkStartPos[c] = tocIn.readLong();
                chunkLastElements[c] = tocIn.readInt();
            }
//            tocIn.close();
        }
    }

    private ChunkContents getChunkForElement(int i) throws IOException {
        for (int c = 0; c < chunkLastElements.length; ++c) {
            if (i < chunkLastElements[c]) {
                SoftReference<ChunkContents> chunkContentsRef = chunkContentsMap.get(c);
                if (chunkContentsRef != null) {
                    ChunkContents chunkContents = chunkContentsRef.get();
                    if (chunkContents != null) {
                        return chunkContents;
                    }
                }
                return readChunk(c);

            }
        }
        throw new IndexOutOfBoundsException("" + i);
    }

    private ChunkContents readChunk(int chunkIndex) throws IOException {
        synchronized (raf) {
            ChunkContents chunkContents = new ChunkContents();
            raf.seek(chunkStartPos[chunkIndex]);
            DataInputStream chunkIn = getInputStream();
            final int numElements = chunkIn.readInt();
            chunkContents.elementStartPositions = new int[numElements];
            for (int e = 0; e < numElements; ++e) {
                chunkContents.elementStartPositions[e] = chunkIn.readInt();
            }
            final int uncompressedSize = chunkIn.readInt();
            chunkContents.firstElement = chunkLastElements[chunkIndex] - numElements;
            chunkContents.decompressedBytes = new byte[uncompressedSize];
            chunkIn.read(chunkContents.decompressedBytes);
            chunkContentsMap.put(chunkIndex, new SoftReference<ChunkContents>(chunkContents));
    //        chunkIn.close();
            return chunkContents;
        }
    }

    @Override
    public T get(final int i) {
        final ChunkContents chunkContents;
        try {
            chunkContents = getChunkForElement(i);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        assert chunkContents != null;
        final int relativePosition = i - chunkContents.firstElement;
        assert relativePosition >= 0;
        assert relativePosition < chunkContents.elementStartPositions.length;
        
        final int startPos = chunkContents.elementStartPositions[relativePosition];
        final DataInputStream in = new DataInputStream(new ByteArrayInputStream(chunkContents.decompressedBytes, startPos, chunkContents.decompressedBytes.length - startPos));
        final T result;
        try {
            result = serializer.read(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
//            try {
//                in.close();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
        }
        return result;
    }

    @Override
    public int size() {
        return chunkLastElements.length == 0 ? 0 : chunkLastElements[chunkLastElements.length - 1];
    }

    public static <T> CompressedList<T> create(final RandomAccessFile raf,
            final Serializer<T> serializer, final long startOffset)
            throws IOException {
        return new CompressedList<T>(raf, serializer, startOffset);
    }

    // /**
    // * Same, but deserialization ignores indices.
    // */
    // public static <T> RAFList<T> create(final RandomAccessFile raf,
    // final Serializer<T> serializer, final long startOffset)
    // throws IOException {
    // return new RAFList<T>(raf, getWrapper(serializer), startOffset);
    // }

    // public static <T> void write(final RandomAccessFile raf,
    // final Collection<T> list, final RAFSerializer<T> serializer)
    // throws IOException {
    // write(raf, list, getWrapper(serializer));
    // }

    // public static <T> RAFListSerializer<T> getWrapper(final RAFSerializer<T>
    // serializer) {
    // return new RAFListSerializer.Wrapper<T>(serializer);
    // }
}
