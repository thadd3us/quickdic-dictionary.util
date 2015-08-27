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

package com.hughes.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class FileUtil {
  
  public static void writeObject(final Object o, final File file) throws FileNotFoundException, IOException {
    final ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(file)));
    oos.writeObject(o);
    oos.close();
  }

  public static void writeObject(final Serializable o, final String file) throws FileNotFoundException, IOException {
    writeObject(o, new File(file));
  }

  public static Object readObject(final File file) throws IOException, ClassNotFoundException {
    final ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(file)));
    final Object result = ois.readObject();
    ois.close();
    return result;
  }

  public static Object readObject(final String file) throws IOException, ClassNotFoundException {
    return readObject(new File(file));
  }
  
  public static <T> T readObject(final File file, final Class<T> class1) throws IOException, ClassNotFoundException {
    return class1.cast(readObject(file));
  }
  
  public static String readLine(final RandomAccessFile file, final long startPos) throws IOException {
    file.seek(startPos);
    return file.readLine();
  }
  
  public static List<String> readLines(final File file) throws IOException {
    final List<String> result = new ArrayList<String>();
    final BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    String line;
    while ((line = in.readLine()) != null) {
      result.add(line);
    }
    in.close();
    return result;
  }
  
  public static String readToString(final File file) throws IOException {
    final BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    StringBuilder result = new StringBuilder();
    String line;
    while ((line = in.readLine()) != null) {
      result.append(line).append("\n");
    }
    in.close();
    return result.toString();
  }
  
  public static void writeStringToUTF8File(final String string, final File file) {
      throw new IllegalStateException();
  }

  public static void printString(final File file, final String s) throws IOException {
      final PrintStream out = new PrintStream(new FileOutputStream(file));
      out.print(s);
      out.close();
    }

}
