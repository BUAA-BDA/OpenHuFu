package com.hufudb.openhufu.data.storage;

import java.util.ArrayList;
import java.util.List;
import com.hufudb.openhufu.data.schema.Schema;

public class ArrayDataSet implements MaterializedDataSet {
  final Schema schema;
  final List<ArrayRow> rows;
  final int rowCount;

  ArrayDataSet(Schema schema, List<ArrayRow> rows) {
    this.schema = schema;
    this.rows = rows;
    this.rowCount = rows.size();
  }

  public static ArrayDataSet materialize(DataSet dataSet) {
    DataSetIterator iterator = dataSet.getIterator();
    List<ArrayRow> rows = new ArrayList<>();
    while (iterator.next()) {
      rows.add(ArrayRow.materialize(iterator));
    }
    dataSet.close();
    return new ArrayDataSet(dataSet.getSchema(), rows);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new Iterator();
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public Object get(int rowIndex, int columnIndex) {
    return rows.get(rowIndex).get(columnIndex);
  }

  @Override
  public int rowCount() {
    return rowCount;
  }

  class Iterator implements DataSetIterator {
    int pointer;

    Iterator() {
      pointer = -1;
    }

    @Override
    public Object get(int columnIndex) {
      return rows.get(pointer).get(columnIndex);
    }

    @Override
    public int size() {
      return schema.size();
    }

    @Override
    public boolean next() {
      ++pointer;
      return pointer < rows.size();
    }

  }
}
