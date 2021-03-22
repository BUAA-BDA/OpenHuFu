package tk.onedb.core.data;

import java.util.ArrayList;
import java.util.List;

public class BasicDataSet extends DataSet {
  List<Row> rows;
  int cursor = 0;
  Row current = null;

  BasicDataSet(Header header, List<Row> rows) {
    super(header);
    this.rows = rows;
  }

  BasicDataSet(Header header) {
    super(header);
    rows = new ArrayList<>();
  }

  @Override
  public Row current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    if (cursor >= rows.size()) {
      return false;
    } else {
      current = rows.get(cursor);
      cursor++;
      return true;
    }
  }

  @Override
  public void reset() {
    cursor = 0;
    current = null;
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  int getRowCount() {
    return rows.size();
  }

  @Override
  void addRow(Row row) {
    rows.add(row);
  }

  @Override
  void addRows(List<Row> rows) {
    rows.addAll(rows);
  }

  @Override
  void mergeDataSet(DataSet dataSet) {
    rows.addAll(dataSet.getRows());
  }

  @Override
  List<Row> getRows() {
    return rows;
  }
}
