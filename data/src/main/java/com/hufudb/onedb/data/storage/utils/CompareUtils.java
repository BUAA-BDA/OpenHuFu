package com.hufudb.onedb.data.storage.utils;

public class CompareUtils {
  private static final int[][] funcTable = {
      {0, -1},
      {1, 0}
  };

  public static int compare(Object a, Object b) {
    boolean b1 = a != null;
    boolean b2 = b != null;
    return b1 & b2 ? ((Comparable) a).compareTo(b) : funcTable[b1 ? 1 : 0][b2 ? 1 : 0];
  }

  public static boolean equal(Object a, Object b) {
    boolean b1 = a != null;
    boolean b2 = b != null;
    return b1 & b2 ? a.equals(b) : funcTable[b1 ? 1 : 0][b2 ? 1 : 0] == 0;
  }
}
