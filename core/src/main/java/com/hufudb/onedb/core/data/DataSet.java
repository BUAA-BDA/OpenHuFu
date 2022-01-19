package com.hufudb.onedb.core.data;

import java.util.List;

public abstract class DataSet {
  final Header header;

  DataSet(Header header) {
    this.header = header;
  }

  public Header getHeader() {
    return header;
  }

  public abstract int getRowCount();

  public abstract void addRow(Row row);

  public abstract void addRows(List<Row> rows);

  public abstract void mergeDataSet(DataSet dataSet);

  abstract List<Row> getRows();
}
