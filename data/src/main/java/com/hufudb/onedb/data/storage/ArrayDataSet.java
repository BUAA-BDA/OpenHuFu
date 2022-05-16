package com.hufudb.onedb.data.storage;

import java.util.ArrayList;
import java.util.List;
import com.hufudb.onedb.data.schema.Schema;

public class ArrayDataSet implements DataSet {
  final Schema schema;
  final List<ArrayRow> rows;

  ArrayDataSet(Schema schema, List<ArrayRow> rows) {
    this.schema = schema;
    this.rows = rows;
  }

  public static ArrayDataSet materialize(DataSet dataSet) {
    DataSetIterator iterator = dataSet.getIterator();
    List<ArrayRow> rows = new ArrayList<>();
    while (iterator.next()) {
      rows.add(ArrayRow.materialize(iterator));
    }
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
