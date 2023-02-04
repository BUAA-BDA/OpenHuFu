package com.hufudb.openhufu.data.storage;

import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;

public interface Column {
  Object getObject(int rowNum);
  boolean isNull(int rowNum);
  ColumnType getType();
  int size();

  interface CellGetter {
    Object get(int rowNum);
  }

  interface CellAppender {
    void append(Object val);
  }
}
