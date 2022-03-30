package com.hufudb.onedb.core.data;

import java.util.List;

public interface DataSet {

  public Header getHeader();

  public abstract int getRowCount();

  public abstract void addRow(Row row);

  public abstract void addRows(List<Row> rows);

  public abstract void mergeDataSet(DataSet dataSet);

  public abstract List<Row> getRows();
}
