package com.hufudb.onedb.core.data;

import java.util.List;

public interface DataSet {

  Header getHeader();

  int getRowCount();

  void addRow(Row row);

  void addRows(List<Row> rows);

  void mergeDataSet(DataSet dataSet);

  List<Row> getRows();
}
