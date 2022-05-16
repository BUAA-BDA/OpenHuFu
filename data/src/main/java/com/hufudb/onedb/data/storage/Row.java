package com.hufudb.onedb.data.storage;

public interface Row {
  Object get(int columnIndex);
  int size();

  public static boolean equals(Row r1, Row r2) {
    if (r1.size() != r2.size()) {
      return false;
    }
    final int size = r1.size();
    for (int i = 0; i < size; ++i) {
      if (!r1.get(i).equals(r2.get(i))) {
        return false;
      }
    }
    return true;
  }

  public interface Getter<T> {
    T get();
  }
}
