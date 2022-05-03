package com.hufudb.onedb.core.data;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface DataSet {
  static final Logger LOG = LoggerFactory.getLogger(DataSet.class);

  Header getHeader();

  int getRowCount();

  void addRow(Row row);

  void addRows(List<Row> rows);

  void mergeDataSet(DataSet dataSet);

  List<Row> getRows();
}
