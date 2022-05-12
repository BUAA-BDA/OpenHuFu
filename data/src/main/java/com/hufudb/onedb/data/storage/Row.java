package com.hufudb.onedb.data.storage;

public interface Row {
  Object get(int columnIndex);
}
