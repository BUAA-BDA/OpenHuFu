package group.bda.federate.data;

import java.util.ArrayList;
import java.util.List;

public class Stream2BatchDataSet extends StreamDataSet {
  final private DataSet dataSet;
  private int rowCount;

  public Stream2BatchDataSet(DataSet dataSet) {
    super(dataSet.header);
    this.dataSet = dataSet;
    this.rowCount = 0;
  }

  @Override
  public int getRowCount() {
    return rowCount;
  }

  @Override
  public void addRow(Row row) {
    dataSet.rows.add(row);
    rowCount++;
  }

  @Override
  public void addRows(List<Row> rows) {
    dataSet.rows.addAll(rows);
    rowCount += rows.size();
  }

  @Override
  public void addDataSet(DataSet dataSet) {
    this.dataSet.rows.addAll(dataSet.rows);
  }

  @Override
  public void close() {
    // do nothing
  }

  public DataSet getDataSet() {
    return dataSet;
  }

  public List<List<Row>> getDivided(int divide) {
    List<List<Row>> dataSets = new ArrayList<>();
    List<Row> rows = dataSet.rows;
    int size = rows.size();
    int head = 0;
    for (int i = 0; i < divide - 1; ++i) {
      dataSets.add(rows.subList(head, head + size / divide));
      head += size / divide;
    }
    dataSets.add(rows.subList(head, size));
    return dataSets;
  }
}
