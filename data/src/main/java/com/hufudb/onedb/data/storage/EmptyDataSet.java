package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.schema.Schema;

public class EmptyDataSet implements DataSet {
  public final static EmptyDataSet INSTANCE = new EmptyDataSet();

  @Override
  public Schema getSchema() {
    return Schema.EMPTY;
  }

  @Override
  public DataSetIterator getIterator() {
    return new DataSetIterator() {
      @Override
      public Object get(int columnIndex) {
        return null;
      }
      @Override
      public boolean next() {
        return false;
      }
    };
  }

  @Override
  public void close() {
    // do nothing
  }
}
