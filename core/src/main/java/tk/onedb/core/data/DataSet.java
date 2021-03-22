package tk.onedb.core.data;

import java.util.List;

import org.apache.calcite.linq4j.Enumerator;

public abstract class DataSet implements Enumerator<Row> {
  final Header header;

  DataSet(Header header) {
    this.header = header;
  }

  public Header getHeader() {
    return header;
  }

  abstract int getRowCount();

  abstract void addRow(Row row);

  abstract void addRows(List<Row> rows);

  abstract void mergeDataSet(DataSet dataSet);

  abstract List<Row> getRows();
}
