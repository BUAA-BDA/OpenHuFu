package group.bda.federate.data;

import java.util.List;

public abstract class StreamDataSet {
  final protected Header header;

  public StreamDataSet(final Header header) {
    this.header = header;
  }

  public Header getHeader() {
    return header;
  }

  public abstract int getRowCount();

  public abstract void addRow(Row row);

  public abstract void addRows(List<Row> rows);

  public abstract void addDataSet(DataSet dataSet);

  public abstract void close();
}
