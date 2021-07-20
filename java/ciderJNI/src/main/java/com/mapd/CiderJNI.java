package com.mapd;

import java.io.IOException;

public class CiderJNI {

  static {
    System.loadLibrary("ciderjni");
  }

  public static native int processBlocks(long dbePtr, String sql, String schema,
                                         long[] dataBuffers, long[] dataNulls,
                                         long[] resultBuffers, long[] resultNulls,
                                         int rowCount);

  public static native long init();

  public static native void close(long dbePtr);
}
