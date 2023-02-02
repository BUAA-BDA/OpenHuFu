package com.hufudb.openhufu.data.storage;

public interface MaterializedDataSet extends DataSet {
  int rowCount();
  Object get(int rowIndex, int columnIndex);
}
