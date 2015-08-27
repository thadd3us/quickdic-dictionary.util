package com.hughes.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipUtil {
    
    public static byte[] Zip(byte[] input) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        GZIPOutputStream compressor = new GZIPOutputStream(result);
        compressor.write(input);
        compressor.close();
        return result.toByteArray();
    }

    public static byte[] Unzip(byte[] input, int resultSize) throws IOException {
        GZIPInputStream decompressed = new GZIPInputStream(new ByteArrayInputStream(input));
        final byte[] result = new byte[resultSize];
        decompressed.read(result);
        return result;
    }
    

}
