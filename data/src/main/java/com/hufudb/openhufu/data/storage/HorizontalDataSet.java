package com.hufudb.openhufu.data.storage;

import java.util.List;
import com.hufudb.openhufu.data.schema.Schema;

/**
 * Horizontal combination of multiple @ProtoDataSet
 */
public class HorizontalDataSet implements MaterializedDataSet {
  final Schema schema;
  final List<ProtoDataSet> slices;
  final int rowCount;

  public HorizontalDataSet(List<ProtoDataSet> dataSets) {
    assert dataSets.size() > 0;
    this.schema = dataSets.get(0).getSchema();
    this.slices = dataSets;
    this.rowCount = dataSets.stream().reduce(0, (c, d) -> c + d.rowCount(), (c1, c2) -> c1 + c2);
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
  public int rowCount() {
    return rowCount;
  }

  @Override
  public Object get(int rowIndex, int columnIndex) {
    // todo: implement this
    throw new UnsupportedOperationException();
  }

  class Iterator implements DataSetIterator {
    int dPointer;
    DataSetIterator it;

    Iterator() {
      dPointer = 0;
      it = slices.get(0).getIterator();
    }

    @Override
    public Object get(int columnIndex) {
      return it.get(columnIndex);
    }

    @Override
    public int size() {
      return schema.size();
    }

    @Override
    public boolean next() {
      if (it.next()) {
        return true;
      } else {
        dPointer++;
        if (dPointer >= slices.size()) {
          return false;
        }
        it = slices.get(dPointer).getIterator();
        return next();
      }
    }
  }
}
