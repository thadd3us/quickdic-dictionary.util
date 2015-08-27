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

package com.hughes.util.raf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;

public interface Serializer<T> {

  public void write(final DataOutputStream out, final T t) throws IOException;

  public T read(final DataInputStream in) throws IOException;

  public static final Serializer<String> STRING = new Serializer<String>() {
    @Override
    public void write(DataOutputStream out, String t) throws IOException {
      out.writeUTF(t);
    }

    @Override
    public String read(DataInputStream in) throws IOException {
      return in.readUTF();
    }
  };

  public static final Serializer<Long> LONG = new Serializer<Long>() {
      @Override
      public void write(DataOutputStream out, Long t) throws IOException {
        out.writeLong(t);
      }

      @Override
      public Long read(DataInputStream in) throws IOException {
        return in.readLong();
      }
    };

  // Serializes any class with a write method and the proper constructor. 
  public static final class DataStreamSerializer<T extends DataStreamSerializable<T>> implements Serializer<T> {
    private final Constructor<T> constructor;
    
    public DataStreamSerializer(final Class<T> clazz) {
      try {
        this.constructor = clazz.getConstructor(DataInputStream.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public void write(DataOutputStream out, T t) throws IOException {
      t.write(out);
    }

    @Override
    public T read(DataInputStream in) throws IOException {
      try {
        return constructor.newInstance(in);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  };

}
