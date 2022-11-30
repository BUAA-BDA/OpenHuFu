package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.storage.utils.CompareUtils;

public interface Row {
  Object get(int columnIndex);
  int size();

  public static boolean equals(Row r1, Row r2) {
    if (r1.size() != r2.size()) {
      return false;
    }
    final int size = r1.size();
    for (int i = 0; i < size; ++i) {
      if (!CompareUtils.equal(r1.get(i), r2.get(i))) {
        return false;
      }
    }
    return true;
  }
}
