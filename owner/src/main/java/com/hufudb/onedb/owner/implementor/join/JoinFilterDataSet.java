package com.hufudb.onedb.owner.implementor.join;

import java.util.List;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.MaterializedDataSet;

public class JoinFilterDataSet implements MaterializedDataSet {
  final MaterializedDataSet source;
  final List<Integer> indices;
  final int rowCount;

  public JoinFilterDataSet(MaterializedDataSet source, List<Integer> indices) {
    this.source = source;
    this.indices = indices;
    this.rowCount = indices.size();
  }

  @Override
  public Schema getSchema() {
    return source.getSchema();
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
    return source.get(indices.get(rowIndex), columnIndex);
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
      return source.get(indices.get(pointer), columnIndex);
    }

    @Override
    public int size() {
      return getSchema().size();
    }

    @Override
    public boolean next() {
      pointer++;
      return pointer < rowCount;
    }
  }
}
